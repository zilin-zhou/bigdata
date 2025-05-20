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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 计算男生比女生多几个
 */
public class CH13lxk1 {
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
        job.setJarByClass(CH13lxk1.class);
        job.setReducerClass(MyReducer.class);
        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, _table_name);
        scans.add(scan);
        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);
        FileOutputFormat.setOutputPath(job, new Path("/ch731"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends TableMapper<Text, Text> {
        public int boycount = 0;
        public int girlcount = 0;

        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell scoreCell = columns.getColumnLatestCell(_family, _q_gender);
            if (scoreCell != null) {
                String gender = Bytes.toString(CellUtil.cloneValue(scoreCell));
                if (gender.equals("男")) {
                    boycount++;
                } else {
                    girlcount++;
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            int diff = boycount - girlcount;
            context.write(new Text("1"), new Text(diff + ""));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }
            context.write(new Text("男生比女生多"), new Text(count + ""));
        }
    }

}
