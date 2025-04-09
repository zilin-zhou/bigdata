package ch4.Group;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
    //这里必须重写无参构造器，调用父类的构造器传入 key的类型，便于自动创建key的实例
    public MyGroup(){
        super(Text.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text v1 = (Text) a;
        Text v2 = (Text) b;
        //返回0表示同一组，非0表示不同组
        return v1.toString().charAt(0)>'c' || v1.toString().equals(v2.toString())?0:1;
    }
}
