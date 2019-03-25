package club.cleland.hadoop_basics.mr;


import club.cleland.hadoop_basics.io.UserWritable;
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

/**
 * 使用mr统计学生成绩
 * 输入:
 *  1,jack,78,15
 *  2,tome,23,16
 *  3,jane,45,14
 *  1,jack,90,15
 *  2,tome,56,16
 *  3,jane,88,14
 *
 * 输出：
 *  jack 168 15
 *  tome 79 16
 *  jane 133 14
 */

public class UserScoreInfo extends Configured implements Tool {
    /**
     * Mapper
     */
    public static class UserScoreInfoMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text mapOutputKey = null;
        private UserWritable mapOutputValue = new UserWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //TODO

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.print("Map keyIn: " + key + " Map keyOut: " + value);
            String[] strs = value.toString().split(",");
            int count = 0;
            for(String str: strs){
                if(count == 0){
                    mapOutputValue.setId(str);
                }else if(count == 1){
                    mapOutputValue.setName(str);
                }else if(count == 2){
                    mapOutputValue.setScore(Integer.parseInt(str));
                }else if(count == 3){
                    mapOutputValue.setAge(Integer.parseInt(str));
                }
                count += 1;
            }
            mapOutputKey = new Text(mapOutputValue.getId());
            context.write(mapOutputKey, new UserWritable("1",  "2", 3, 5));

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
        job.setMapperClass(UserScoreInfoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserWritable.class);

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
        args = new String[]{
                "hdfs://loc-header:9000//data/input/user_info.csv",
                "hdfs://loc-header:9000//data/output/"
        };
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