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

public class MRTemplate extends Configured implements Tool{

    /**
     * map
     * TODO
     */
    public static class TempleteMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        public  void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //TODO
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //TODO
        }

    }


    /**
     * reduce
     * TODO
     */
    public static class TempleteReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //TODO
        }
    }


    /**
     * run
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String args[]) throws Exception {

        //driver
        //1) get conf
        Configuration configuration = this.getConf();

        //2) create job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        //3.1)  input
        Path path = new Path(args[0]);
        FileInputFormat.addInputPath(job, path);


        //3.2) map

        job.setMapperClass(TempleteMapper.class);
        //TODO
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //1.分区
        //job.setPartitionerClass();

        //2.排序
        //job.setSortComparatorClass();

        //3.combiner
        //job.setCombinerClass(WordCountCombiner.class);

        //4.compress
        //configuration.set("mapreduce.map.output.compress","true");
        //configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        //5.分组
        //job.setGroupingComparatorClass();


        //3.3) reduce
        job.setReducerClass(TempleteReduce.class);
        //TODO
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        //3.4) output
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);

        //4) commit
        boolean isSuc = job.waitForCompletion(true);
        return (isSuc) ? 0 : 1;
    }

    public static void main(String[] args) {
        args = new String[]{
                "hdfs://loc-header:9000/data/input/wordcount.txt",
                "hdfs://loc-header:9000/data/output/mr_template"
        };

        Configuration configuration = new Configuration();
        try {

            //先判断
            Path fileOutPath = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(configuration);
            if(fileSystem.exists(fileOutPath)){
                fileSystem.delete(fileOutPath,true);
            }

            //int status = wordCountMR.run(args);
            int status = ToolRunner.run(configuration,new MRTemplate(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
