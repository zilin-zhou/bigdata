package ch13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 计算学生年龄并输出到students_age
 */
public class CH13MRStudentsAge {
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
        job.setJarByClass(CH13MRStudentsAge.class);
        job.setReducerClass(MyReducer.class);
        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, _table_name);
        scans.add(scan);
        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("students_age", MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class MyMapper extends TableMapper<Text, Text> {
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell birthdayCell = columns.getColumnLatestCell(_family, _q_birthday);
            if (birthdayCell != null) {
                String birthday = Bytes.toString(CellUtil.cloneValue(birthdayCell));
                Integer birthYear = Integer.parseInt(birthday.split("-")[0]);
                Integer age = 2022 - birthYear;
                context.write(new Text(Bytes.toString(key.get())), new Text(age + ""));
            }
        }
    }

    static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {
            Put put = new Put(key.getBytes());
            put.addColumn(_family, Bytes.toBytes("age"), Bytes.toBytes(values.iterator().next().toString()));
            context.write(new ImmutableBytesWritable(key.getBytes()), put);
        }
    }
}
