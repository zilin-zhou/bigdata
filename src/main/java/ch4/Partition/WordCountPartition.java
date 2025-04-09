package ch4.Partition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountPartition {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        System.setProperty("HADOOP_USER_NAME","root");
        Job job = Job.getInstance(conf, "partition");

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置分区函数
        job.setPartitionerClass(MyPartition.class);
        //设置reduce个数
        job.setNumReduceTasks(2);
        //设置 Mapper和Reducer类
        FileInputFormat.setInputPaths(job, new Path("hdfs://node01:9000/partition/input/diypartition.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://node01:9000/partition/output/"));
        job.waitForCompletion(true);
    }

}
class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //注意要根据自己的数据设置对应的分隔符
        String[] strs = value.toString().split(" ");
        for(String str:strs) {
            context.write(new Text(str), new IntWritable(1));
        }
    }
}
class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v:values) {
            sum+=v.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

