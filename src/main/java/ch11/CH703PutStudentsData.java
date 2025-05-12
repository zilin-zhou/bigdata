package ch11;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CH703PutStudentsData extends CH701HBaseBase {
    public void run() throws IOException {
        // 检查学生表是否存在，如果存在则清空表
        TableName tableName = TableName.valueOf("students");
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        // 创建学生表
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build();
        builder.setColumnFamily(familyDescriptor);
        TableDescriptor tableDescriptor = builder.build();
        admin.createTable(tableDescriptor);
        System.out.println(Arrays.asList(admin.listTableNames()));
        // 获取table对象
        Table table = conn.getTable(tableName);
        // 写入一条数据
        //a)	put "students","1","data:sex","male"
        //b)	put "students","2","data:sex","male"
        //c)	put "students","1","data:birthday","2003-04-06"
        //d)	put "students","2","data:birthday","2003-10-11"
        //e)	put "students","1","data:home","kaifeng"
        //f)	put "students","2","data:home","anyang"
        //g)	put "students","1","data:dorm","1#101"
        //h)	put "students","2","data:dorm","1#101"
        Put put = new Put(Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-04-06"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("kaifeng"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
        table.put(put);
        // 写入一串数据
        List<Put> putList = new ArrayList<Put>();
        put = new Put(Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-10-11"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("anyang"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
        putList.add(put);
        put = new Put(Bytes.toBytes("3"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("sunzi"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sex"), Bytes.toBytes("female"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-09-15"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("shangqiu"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
        putList.add(put);
        put = new Put(Bytes.toBytes("4"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhouba"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sex"), Bytes.toBytes("female"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2002-07-08"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("zhoukou"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
        putList.add(put);
        table.put(putList);
    }

    public static void main(String[] args) throws IOException {
        new CH703PutStudentsData().run();
    }
}
