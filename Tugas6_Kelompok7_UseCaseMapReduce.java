import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ApiSlaProfiler {

    // ==========================================
    // 1. MAPPER CLASS
    // ==========================================
    public static class ApiSlaMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text endpointKey = new Text();
        private Text metricValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("#")) {
                return; // Abaikan baris kosong atau komentar
            }

            // Skema: [Timestamp] [Endpoint] [Method] [StatusCode] [LatencyMS]
            String[] tokens = line.split("\\s+");
            if (tokens.length >= 5) {
                String endpoint = tokens[1];
                String statusCodeStr = tokens[3];
                String latencyStr = tokens[4];

                try {
                    // Validasi integritas data numerik
                    int statusCode = Integer.parseInt(statusCodeStr);
                    double latency = Double.parseDouble(latencyStr);

                    endpointKey.set(endpoint);
                    // Emit format: "StatusCode,Latency"
                    metricValue.set(statusCode + "," + latency);
                    context.write(endpointKey, metricValue);
                } catch (NumberFormatException e) {
                    // Melewati record yang korup/anomali
                }
            }
        }
    }

    // ==========================================
    // 2. REDUCER CLASS
    // ==========================================
    public static class ApiSlaReducer extends Reducer<Text, Text, Text, Text> {
        private Text reportResult = new Text();

        // Ambang batas SLA Organisasi
        private static final double MAX_ALLOWED_AVG_LATENCY = 300.0; // ms
        private static final double MAX_ALLOWED_ERROR_RATE = 1.0;     // %
        private static final double CRITICAL_LATENCY_THRESHOLD = 500.0; // ms

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            long totalRequests = 0;
            long serverErrors = 0;
            long criticalLatencyCount = 0;
            double sumLatency = 0.0;
            double peakLatency = Double.MIN_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 2) {
                    int statusCode = Integer.parseInt(parts[0]);
                    double latency = Double.parseDouble(parts[1]);

                    totalRequests++;
                    sumLatency += latency;

                    if (latency > peakLatency) {
                        peakLatency = latency;
                    }
                    if (statusCode >= 500) {
                        serverErrors++;
                    }
                    if (latency > CRITICAL_LATENCY_THRESHOLD) {
                        criticalLatencyCount++;
                    }
                }
            }

            if (totalRequests > 0) {
                double avgLatency = sumLatency / totalRequests;
                double errorRate = ((double) serverErrors / totalRequests) * 100.0;

                // Evaluasi Kepatuhan SLA
                boolean isCompliant = (errorRate <= MAX_ALLOWED_ERROR_RATE) && 
                                      (avgLatency <= MAX_ALLOWED_AVG_LATENCY);
                String status = isCompliant ? "COMPLIANT [SLA MET]" : "BREACH [SLA VIOLATED]";

                String summary = String.format(
                    "Reqs: %d | ErrRate: %.2f%% | AvgLat: %.2fms | PeakLat: %.2fms | LatViolations(>500ms): %d | Status: %s",
                    totalRequests, errorRate, avgLatency, peakLatency, criticalLatencyCount, status
                );

                reportResult.set(summary);
                context.write(key, reportResult);
            }
        }
    }

    // ==========================================
    // 3. DRIVER METHOD
    // ==========================================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: ApiSlaProfiler <input-path-hdfs> <output-path-hdfs>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Microservices API SLA & Performance Auditor");
        job.setJarByClass(ApiSlaProfiler.class);

        job.setMapperClass(ApiSlaMapper.class);
        job.setReducerClass(ApiSlaReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
