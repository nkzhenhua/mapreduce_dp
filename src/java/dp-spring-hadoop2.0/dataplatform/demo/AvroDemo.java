import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AvroDemo extends Configured implements Tool {
    public static class ColorCountMapper extends Mapper<AvroKey<my_record>, NullWritable, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(AvroKey<my_record> key, NullWritable value, Context context) throws IOException,
                InterruptedException {

            System.out.println("country in map ");
            my_record aRecord = key.datum();
            CharSequence country = aRecord.getCountry();
            System.out.println("country in map: " + country.toString());

            context.write(new Text(country.toString()), one);
        }
    }

    public static class ColorCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            System.out.println("country in reduce: " + key.toString() + "value:" + sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] rawArgs) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJarByClass(AvroDemo.class);
        job.setJobName("Color Count");
        String[] args = new GenericOptionsParser(rawArgs).getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(ColorCountMapper.class);
        AvroJob.setInputKeySchema(job, my_record.SCHEMA$);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(ColorCountReducer.class);

        // AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        // AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AvroDemo(), args);
        System.exit(res);
    }

}
