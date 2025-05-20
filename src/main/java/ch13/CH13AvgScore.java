package ch13;

import ch11.CH701HBaseBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 任务四：（均值）求平均分数
 */
public class CH13AvgScore extends CH701HBaseBase {
    public void run() throws IOException {
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
        ResultScanner scanner = table.getScanner(scan);
        int totalScore = 0, count = 0;
        for (Result res : scanner) {
            String resString = Bytes.toString(res.getValue(Bytes.toBytes("data"), Bytes.toBytes("score")));
            if (resString.length() > 0) {
                int num = Integer.parseInt(resString);
                count++;
                totalScore += num;
            }
        }
        System.out.println("平均分" + (double)totalScore/count);
        conn.close();
    }

    public static void main(String[] args) {
        try {
            String className = Thread.currentThread().getStackTrace()[1].getClassName();
            Class<? extends CH701HBaseBase> clazz = (Class<? extends CH701HBaseBase>) Class.forName(className);
            clazz.newInstance().run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
