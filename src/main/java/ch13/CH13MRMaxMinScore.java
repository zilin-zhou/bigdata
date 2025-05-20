package ch13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 输出成绩的最大值最小值
 */
public class CH13MRMaxMinScore {
    private static final byte[] _table_name = Bytes.toBytes("students");
    private static final byte[] _family = Bytes.toBytes("data");
    private static final byte[] _q_name = Bytes.toBytes("name");
    private static final byte[] _q_clazz = Bytes.toBytes("clazz");
    private static final byte[] _q_sid = Bytes.toBytes("sid");
    private static final byte[] _q_gender = Bytes.toBytes("gender");
    private static final byte[] _q_birthday = Bytes.toBytes("birthday");
    private static final byte[] _q_phone = Bytes.toBytes("phone");
    private static final byte[] _q_loc = Bytes.toBytes("loc");
    private static final byte[] _q_score = Bytes.toBytes("score");

    public static void main(String[] args) throws Exception {
        String hadoop_home = "E:\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(CH13MRMaxMinScore.class);
        job.setReducerClass(MyReducer.class);
        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, _table_name);
        scans.add(scan);
        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, IntWritable.class, job);
        FileOutputFormat.setOutputPath(job, new Path("/ch722"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class MyMapper extends TableMapper<Text, IntWritable> {
        private int maxScore = Integer.MIN_VALUE, minScore = Integer.MAX_VALUE;
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell scoreCell = columns.getColumnLatestCell(_family, _q_score);
            if (scoreCell != null) {
                Integer score = Integer.parseInt(Bytes.toString(CellUtil.cloneValue(scoreCell)));
                if (score > maxScore) {
                    maxScore = score;
                }
                if (score < minScore) {
                    minScore = score;
                }
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("score"), new IntWritable(maxScore));
            context.write(new Text("score"), new IntWritable(minScore));
        }
    }

    private static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int maxScore = Integer.MIN_VALUE, minScore = Integer.MAX_VALUE;
            for (IntWritable value : values) {
                int num = value.get();
                if (num > maxScore) {
                    maxScore = num;
                }
                if (num < minScore) {
                    minScore = num;
                }
            }
            context.write(new Text("max:" + maxScore), new Text("min:" + minScore));
        }
    }


}
