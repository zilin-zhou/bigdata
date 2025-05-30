package ch11;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Ch704GetAndScanStudentsData extends CH701HBaseBase {
    public void run() throws IOException {
        // 获取table对象
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        // 使用get命令查看
        Get get = new Get(Bytes.toBytes("1"));
        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));
        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"));
        Result result = table.get(get);
        System.out.println("1.name: " + Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"))));
        System.out.println("1.birthday: " + Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("birthday"))));
        System.out.println("1.home: " + Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("home"))));
        // 使用scan命令检索
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"));
        scan.setFilter(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("2"))));
        ResultScanner scanner = table.getScanner(scan);
        for (Result res : scanner) {
            System.out.println("2.name: " + Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"))));
            System.out.println("2.birthday: " + Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("birthday"))));
            System.out.println("2.home: " + Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("home"))));
        }
    }

    public static void main(String[] args) throws IOException {
        new Ch704GetAndScanStudentsData().run();
    }
}
