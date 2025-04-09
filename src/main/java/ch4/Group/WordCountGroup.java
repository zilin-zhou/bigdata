package ch4.Group;

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

public class WordCountGroup {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        System.setProperty("HADOOP_USER_NAME","root");
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf,"MR");

        //1. 设置Mapper,Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //2. 设置输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //3. 设置分组函数
        job.setGroupingComparatorClass(MyGroup.class);

        FileInputFormat.addInputPath(job,new Path("hdfs://node01:9000/group/input/group.txt"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://node01:9000/group/output/"));

        job.waitForCompletion(true);

    }
}

class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
    /*
     * 每读一行文本，就会执行一次 map 方法
     * key:是这一行文本的偏移量,这一行文本的标识
     * value:这一行文本的内容
     * context：上下文
     * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(key+"-----------------");
        //1. 将value转化为String
        String line = value.toString();
        //2. 将文本进行分割
        String[] words = line.split("\t");
        for(String word:words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}

class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable v:values){
            count+=v.get();
        }
        context.write(key,new IntWritable(count));
    }
}