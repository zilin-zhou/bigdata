package ch6.allsort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AllSort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        System.setProperty("HADOOP_USER_NAME","root");
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf,"MR");

        //1. 设置Mapper,Reducer
        job.setMapperClass(AllSortMapper.class);
        job.setReducerClass(AllSortReducer.class);


        //2. 设置输出的类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.addInputPath(job,new Path("file:///hadoop/allsort/in/"));
        FileOutputFormat.setOutputPath(job,new Path("file:///hadoop/allsort/out/"));

        job.waitForCompletion(true);

    }
}

class AllSortMapper extends Mapper<LongWritable, Text,LongWritable, NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        context.write(new LongWritable(Long.valueOf(line)),NullWritable.get());
    }
}

class AllSortReducer extends Reducer<LongWritable,NullWritable,LongWritable,NullWritable>{
    @Override
    protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
