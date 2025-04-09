package ch4.Sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator {
    //这里必须重写无参构造方法，调用父类的构造方法传入 key的类型，便于自动创建key的实例
    public MySort(){
        super(Text.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text v1 = (Text) a;
        Text v2 = (Text) b;
        //正常顺序为升序，逆向为降序
        return -v1.compareTo(v2);
    }
}
