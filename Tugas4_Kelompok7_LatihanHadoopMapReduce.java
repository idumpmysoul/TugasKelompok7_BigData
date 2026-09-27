import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextStats {

    // ================= MAPPER =================
    // Input : <Offset baris (LongWritable), Baris teks (Text)>
    // Output: <Label/Key (Text), Nilai numerik (DoubleWritable)>
    public static class StatMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text categoryKey = new Text();
        private DoubleWritable numValue = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            // Parsing baris teks (Format yang diharapkan: "Key Nilai" atau sekadar deretan "Nilai")
            StringTokenizer tokenizer = new StringTokenizer(line);
            
            if (tokenizer.countTokens() >= 2) {
                // Kasus 1: Baris berisi pasangan "Kategori Angka" (Contoh: "KelasA 85.5")
                String keyStr = tokenizer.nextToken();
                String valStr = tokenizer.nextToken();
                try {
                    double val = Double.parseDouble(valStr);
                    categoryKey.set(keyStr);
                    numValue.set(val);
                    context.write(categoryKey, numValue);
                } catch (NumberFormatException e) {
                    // Melewati data non-angka
                }
            } else if (tokenizer.countTokens() == 1) {
                // Kasus 2: Baris hanya berisi angka saja (Contoh: "85.5"), kita satukan di label "GLOBAL"
                String valStr = tokenizer.nextToken();
                try {
                    double val = Double.parseDouble(valStr);
                    categoryKey.set("GLOBAL");
                    numValue.set(val);
                    context.write(categoryKey, numValue);
                } catch (NumberFormatException e) {
                    // Melewati token bukan angka
                }
            }
        }
    }

    // ================= REDUCER =================
    // Input : <Label (Text), Kumpulan Nilai (Iterable<DoubleWritable>)>
    // Output: <Label (Text), Hasil Stat (Text)>
    public static class StatReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            double sum = 0.0;
            long count = 0;

            for (DoubleWritable val : values) {
                double currentVal = val.get();
                
                // Cari Min
                if (currentVal < min) {
                    min = currentVal;
                }
                
                // Cari Max
                if (currentVal > max) {
                    max = currentVal;
                }
                
                // Akumulasi untuk Rata-rata
                sum += currentVal;
                count++;
            }

            if (count > 0) {
                double average = sum / count;
                String outputStr = String.format("Min: %.2f | Max: %.2f | Avg: %.2f | Count: %d", min, max, average, count);
                result.set(outputStr);
                context.write(key, result);
            }
        }
    }

    // ================= DRIVER =================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: TextStats <input-path> <output-path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Text Statistics (Min, Max, Avg)");
        job.setJarByClass(TextStats.class);

        job.setMapperClass(StatMapper.class);
        job.setReducerClass(StatReducer.class);

        // Tipe keluaran dari Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Tipe keluaran akhir dari Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
