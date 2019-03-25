package club.cleland.hadoop_basics.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class MRTemplate extends Configured implements Tool {
    /**
     * Mapper
     */
    public static class TemplateMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //TODO

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //TODO
        }
    }

    // Reduce
    public static class TemplateReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //TODO
        }
    }


    /**
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        // get config
        Configuration configuration = new Configuration();

        // create job
        Job job = Job.getInstance(configuration, "template");
        job.setJarByClass(this.getClass());

        // input
        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputPath);

        // map
        job.setMapperClass(TemplateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 1. partition分区
        //job.setPartitionerClass();

        // 2. 排序
        //job.setSortComparatorClass();

        // 3. combiner
        //job.setCombinerClass();

        // 4. compress
        //configuration.set("mapreduce.map.output.compress","true");
        //configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        // 5. group分组
        //job.setGroupingComparatorClass();

        // reduce
        job.setReducerClass(TemplateReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // output
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        // commit
        boolean isSuc = job.waitForCompletion(true);
        return (isSuc) ? 0 : 1;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args){
        Configuration configuration = new Configuration();
        try{
            Path fileOutPath = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(configuration);
            if(fileSystem.exists(fileOutPath)){
                fileSystem.delete(fileOutPath, true);
            }

            int status = ToolRunner.run(configuration, new MRTemplate(), args);
            System.exit(status);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}