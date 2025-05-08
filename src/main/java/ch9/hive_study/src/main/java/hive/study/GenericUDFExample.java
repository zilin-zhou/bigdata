package hive.study;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class GenericUDFExample extends GenericUDF {
    private PrimitiveObjectInspector stringOI1;
    private PrimitiveObjectInspector stringOI2;
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        stringOI1 = (PrimitiveObjectInspector) arguments[0];
        stringOI2 = (PrimitiveObjectInspector) arguments[1];
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // 获取第一个参数的值
        Object arg1 = arguments[0].get();
        // 检测该参数是否为空
        if (arg1 == null) {
            return false;
        }
        // 获取第二个参数的值
        Object arg2 = arguments[1].get();
        // 比较两个参数的值是否相等
        return stringOI1.getPrimitiveJavaObject(arg1)
                .equals(stringOI2.getPrimitiveJavaObject(arg2));
    }

    @Override
    public String getDisplayString(String[] children) {
        return "GenericUDFExample(" + children[0] + ", " + children[1] + ")";
    }
}
