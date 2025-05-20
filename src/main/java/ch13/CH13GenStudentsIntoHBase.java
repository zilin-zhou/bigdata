package ch13;

import ch11.CH701HBaseBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 模拟学生数据导入HBase
 */
public class CH13GenStudentsIntoHBase extends CH701HBaseBase {

    public String loadFileToString(String filename) throws Exception {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(new FileReader(filename));
        while (br.ready()) {
            sb.append(br.readLine().trim());
        }
        br.close();
        return sb.toString();
    }

    public String[] loadFileToArr(String filename) throws Exception {
        List<String> arrList = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(filename));
        while (br.ready()) {
            String line = br.readLine().trim();
            if (line.length() > 0) {
                arrList.add(line);
            }
        }
        br.close();
        return arrList.toArray(new String[arrList.size()]);
    }

    @Override
    public void run() {
        try {
            // 百家姓
            char[] family_names = loadFileToString("D:\\JavaProject\\myhadoop\\src\\main\\java\\ch13\\data\\family_names.txt").toCharArray();
            // 常用字
            char[] common_words = loadFileToString("D:\\JavaProject\\myhadoop\\src\\main\\java\\ch13\\data\\common_words.txt").toCharArray();
            // 生成班级
            int clazzLength = 100;
            String[] clazz = new String[clazzLength];
            for (int i = 0; i < clazzLength; i++) {
                clazz[i] = "RB" + String.format("%03d", i);
            }
            String[] gender = {"男", "女"};
            // 生成出生年月日，截取3年的
            List<String> birthdayList = new ArrayList<>(4 * 365);
            long from = System.currentTimeMillis() - 22l * 365 * 24 * 3600 * 1000;
            long to = System.currentTimeMillis() - 19l * 365 * 24 * 3600 * 1000;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            for (long t = from; t < to; t += 24l * 3600 * 1000) {
                birthdayList.add(sdf.format(new Date(t)));
            }
            String[] birthday = birthdayList.toArray(new String[birthdayList.size()]);
            // 加载常用电话号段（前3位，后8位随机生成）
            String[] phone = loadFileToArr("D:\\JavaProject\\myhadoop\\src\\main\\java\\ch13\\data\\phone_start.txt");
            // 生成省份和地市，门牌号随机生成
            String[] city = loadFileToArr("D:\\JavaProject\\myhadoop\\src\\main\\java\\ch13\\data\\citys.txt");
            // 随机生成学生，每班1W条
            Random rand = new Random();
            Table table = conn.getTable(TableName.valueOf("students"));
            List<Put> putList = new ArrayList<>();
            for (int i = 0; i < clazzLength * 10000; i++) {
                String stuClazz = clazz[i / 10000];
                String sid = stuClazz + String.format("%04d", i % 10000);
                Put put = new Put(Bytes.toBytes(sid));
                StringBuilder randName = new StringBuilder();
                randName.append(family_names[rand.nextInt(family_names.length)]);
                randName.append(common_words[rand.nextInt(common_words.length)]);
                randName.append(common_words[rand.nextInt(common_words.length)]);
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes(randName.toString()));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("clazz"), Bytes.toBytes(stuClazz));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sid"), Bytes.toBytes(sid));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes(gender[rand.nextInt(gender.length)]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes(birthday[rand.nextInt(birthday.length)]));
                String randPhone = phone[rand.nextInt(phone.length)] + String.format("%08d", rand.nextInt(100000000));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("phone"), Bytes.toBytes(randPhone));
                String randLoc = city[rand.nextInt(city.length)] + String.format("%03d", rand.nextInt(1000)) + "号";
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("loc"), Bytes.toBytes(randLoc));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"), Bytes.toBytes(rand.nextInt(100)+""));
                putList.add(put);
                if (i % 10000 == 9999) {
                    table.put(putList);
                    putList.clear();
                    System.out.println(stuClazz + "导入完成");
                }
            }
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new CH13GenStudentsIntoHBase().run();
    }
}
