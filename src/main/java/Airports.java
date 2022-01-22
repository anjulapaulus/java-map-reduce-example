import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Airports {
    public static class StatusMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text flightOrigin = new Text();
        private final static IntWritable intWritableOne = new IntWritable(1);

        public void map(Object key, Text val, Context ctx) throws IOException, InterruptedException, NumberFormatException {
            String[] colmns = val.toString().split(",", -1);

            String origin = colmns[5];
            flightOrigin.set(origin);

            ctx.write(flightOrigin, intWritableOne);
        }

    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable res = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            res.set(count);
            ctx.write(key, res);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] extraArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (extraArgs.length != 2) {
            System.err.println("Usage: Flight Analysis <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "flight data");
        job.setJarByClass(Airports.class);
        job.setMapperClass(Airports.StatusMapper.class);
        job.setReducerClass(Airports.SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(extraArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(extraArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
