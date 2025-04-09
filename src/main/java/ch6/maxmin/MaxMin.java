package ch6.maxmin;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.TreeSet;

public class MaxMin{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        System.setProperty("HADOOP_USER_NAME","root");
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf,"MR");

        //1. 设置Mapper,Reducer
        job.setMapperClass(MaxMinMapper.class);
        job.setReducerClass(MaxMinReducer.class);

        //2. 设置输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path("file:///hadoop/maxmin/in/"));
        FileOutputFormat.setOutputPath(job,new Path("file:///hadoop/maxmin/out/"));

        job.waitForCompletion(true);
    }
}

class MaxMinMapper extends Mapper<LongWritable, Text,LongWritable, NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        context.write(new LongWritable(Long.valueOf(line)),NullWritable.get());
    }
}

class MaxMinReducer extends Reducer<LongWritable,NullWritable,LongWritable,LongWritable>{
    private TreeSet<LongWritable> result = new TreeSet<LongWritable>();
    @Override
    protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //添加元素到 Result中
        result.add(key);
        //如果元素的个数为3，表示第一个为最小值，最后一个为最大值，删除中间的那个即可
        if(result.size() == 3){
            Object[] tmp = result.toArray();
            result.remove(tmp[1]);
            tmp = null;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LongWritable[] tmp = new LongWritable[2];
        result.toArray(tmp);
        //最后才输出最值
        context.write(tmp[0],tmp[1]);
    }
}
