import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

    // Mapper Class
    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text year = new Text();
        private FloatWritable temperature = new FloatWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the line by whitespace
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                year.set(parts[0]);
                try {
                    temperature.set(Float.parseFloat(parts[1]));
                    context.write(year, temperature);
                } catch (NumberFormatException e) {
                    // Skip invalid temperature values
                }
            }
        }
    }

    // Reducer Class
    public static class MaxTemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable maxTemperature = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float maxTemp = Float.MIN_VALUE;
            for (FloatWritable val : values) {
                maxTemp = Math.max(maxTemp, val.get());
            }
            maxTemperature.set(maxTemp);
            context.write(key, maxTemperature);
        }
    }

    // Driver Main Method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Temperature By Year");
        job.setJarByClass(MaxTemperature.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
