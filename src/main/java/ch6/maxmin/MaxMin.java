package ch6.maxmin;

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

public class MaxMin {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "root");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "MR");
        job.setJarByClass(MaxMin.class);

        // 1. 设置 Mapper 和 Reducer
        job.setMapperClass(MaxMinMapper.class);
        job.setReducerClass(MaxMinReducer.class);

        // 2. 修正类型声明
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // 3. 输入输出路径
        FileInputFormat.addInputPath(job, new Path("hdfs://node01:9000/maxmin/input/maxmin.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://node01:9000/maxmin/output/"));

        job.waitForCompletion(true);
    }
}

class MaxMinMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        context.write(new LongWritable(Long.parseLong(line)), NullWritable.get());
    }
}

class MaxMinReducer extends Reducer<LongWritable, NullWritable, LongWritable, LongWritable> {
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;

    @Override
    protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) {
        long num = key.get();
        if (num < min) min = num;
        if (num > max) max = num;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(min), new LongWritable(max));
    }
}


