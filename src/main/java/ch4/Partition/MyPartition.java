package ch4.Partition;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
public class MyPartition extends Partitioner<Text, IntWritable>{
    @Override
    public int getPartition(Text key,  IntWritable value, int numPartitions) {
        return key.toString().charAt(0)>'h'?1:0;
    }
}
