package ch13;

import ch11.CH701HBaseBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;

/**
 * 任务二：（去重）求学生中出现的所有姓氏
 */
public class CH13Job2AllFamilyNames extends CH701HBaseBase {
    public void run() throws IOException {
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        ResultScanner scanner = table.getScanner(scan);
        HashSet<String> familyNameSet = new HashSet<>();
        for (Result res : scanner) {
            String resString = Bytes.toString(res.getValue(Bytes.toBytes("data"), Bytes.toBytes("name")));
            if(resString.length()>0){
                familyNameSet.add(resString.split("")[0]);
            }
        }
        System.out.println(familyNameSet);
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
