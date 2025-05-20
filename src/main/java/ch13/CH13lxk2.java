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
import java.util.*;

/**
 * 出生在哪一天的人数第三多？MR任务
 */
public class CH13lxk2 {
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
        job.setJarByClass(CH13lxk2.class);
        job.setReducerClass(MyReducer.class);
        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, _table_name);
        scans.add(scan);
        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);
        FileOutputFormat.setOutputPath(job, new Path("/ch732"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    private static class MyMapper extends TableMapper<Text, Text> {
        public HashMap<String, Integer> map = new HashMap<>();

        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell cell = columns.getColumnLatestCell(_family, _q_birthday);
            if (cell != null) {
                String birthday = Bytes.toString(CellUtil.cloneValue(cell));
                String date = birthday.substring(8);
                Integer count = map.get(date);
                if (count != null) {
                    map.put(date, count + 1);
                } else {
                    map.put(date, 1);
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                context.write(new Text("1"), new Text(entry.getKey() + "\t" + entry.getValue() + ""));
            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<>();
            for (Text value : values) {
                String[] toks = value.toString().split("\t");
                Integer count = map.get(toks[0]);
                if (count != null) {
                    map.put(toks[0], count + Integer.parseInt(toks[1]));
                } else {
                    map.put(toks[0], Integer.parseInt(toks[1]));
                }
            }
            List<Map.Entry<String, Integer>> sortlist = new ArrayList<>(map.entrySet());
            sortlist.sort(new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });
            context.write(new Text("第三多"), new Text(sortlist.get(2) + ""));
            context.write(new Text("整个map"), new Text(sortlist.toString()));
        }
    }


}
