package ch13;

import ch11.CH701HBaseBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 任务一：（计数）统计男生和女生的总人数
 */
public class CH13Job1Count extends CH701HBaseBase {
    public void run() throws IOException {
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"));
        ResultScanner scanner = table.getScanner(scan);
        int boyCount = 0, girlCount = 0;
        for (Result res : scanner) {
            String resString = Bytes.toString(res.getValue(Bytes.toBytes("data"), Bytes.toBytes("gender")));
            if (resString.equals("男")) {
                boyCount++;
            } else {
                girlCount++;
            }
        }
        System.out.println("男生人数" + boyCount);
        System.out.println("女生人数" + girlCount);
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
