package hive.study;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

@Description(name = "split_string", value = "Splits a string into multiple rows")
public class SplitStringUDTF extends GenericUDTF {

    private transient StringObjectInspector inputOI;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        // 检查输入参数个数和类型
        if (argOIs.length != 1) {
            throw new UDFArgumentException("split_string() takes exactly one argument");
        }

        // 确保输入参数为字符串类型
        if (!(argOIs[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("split_string() argument must be a string");
        }

        // 记录输入参数的ObjectInspector
        inputOI = (StringObjectInspector) argOIs[0];

        // 定义输出表的列名和类型
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("value");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        // 获取输入参数的值
        String input = inputOI.getPrimitiveJavaObject(args[0]);

        // 拆分字符串并输出每个拆分的值
        if (input != null) {
            String[] parts = input.split(",");
            for (String part : parts) {
                forward(new Object[]{part});
            }
        }
    }

    @Override
    public void close() throws HiveException {
        // 空实现
    }
}

