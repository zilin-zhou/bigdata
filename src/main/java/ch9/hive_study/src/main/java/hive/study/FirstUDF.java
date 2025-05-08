package hive.study;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

public class FirstUDF extends GenericUDF {
    private transient ObjectInspector value0I;
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;
    private BooleanWritable result;
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //判断是否输入的参数为2
        if (objectInspectors.length != 2){
            throw new UDFArgumentException("args must accept 2 args");
        }
        //判断第一个参数是否是list
        if (!(objectInspectors[0].getCategory().equals(ObjectInspector.Category.LIST))){
            throw new UDFArgumentTypeException(0, "\"array\" expected at function ARRAY_CONTAINS, but \""
                    + objectInspectors[0].getTypeName() + "\" " + "is found");
        }
        //将参数赋值给私有变量
        this.arrayOI = ((ListObjectInspector) objectInspectors[0]);
        this.arrayElementOI=this.arrayOI.getListElementObjectInspector();
        this.value0I= objectInspectors[1];
        //数组元素是否与第二个参数类型相同
        if(!(ObjectInspectorUtils.compareTypes(this.arrayOI,this.value0I))) {
            throw new UDFArgumentTypeException(1,
                    "\"" + this.arrayElementOI.getTypeName() + "\"" + " expected " +
                            "at function ARRAY_CONTAINS, but "
                            + "\"" + this.value0I.getTypeName() + "\"" + " is found");
        }
        //判断ObjectInspector是否支持第二个参数类型
        if (!(ObjectInspectorUtils.compareSupported(this.value0I))) {
            throw new UDFArgumentException("The function ARRAY_CONTAINS does not support comparison for \""
                    + this.value0I.getTypeName() + "\"" + " types");

        }
        this.result=new BooleanWritable(true);
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        this.result.set(false);
        Object array= deferredObjects[0].get();
        Object value= deferredObjects[1].get();
        Integer arrayLength = this.arrayOI.getListLength(array);
        //传入第二个参数是否为nul,或者传入参数长度为0 检验传入参数
        if (value == null || arrayLength<=0){
            return this.result;
        }
        //遍历array中的类型，判断是否与第二个参数相等
        for (int i=0;i<arrayLength;i++) {
            Object listElement = this.arrayOI.getListElement(array, i);
            //判断包含如果本次循环的数组元数为null,或者没有匹配成功，跳过本次循环
            if (listElement == null || ObjectInspectorUtils.compare(value,value0I,listElement,arrayOI) != 0){
                continue;
            }
            //如果匹配成功，将result设置为true
            result.set(true);
            break;
        }
        return result;
    }
    public String getDisplayString(String[] strings) {
        assert (strings.length == 2);
        return "array_contains(" + strings[0] + ", " + strings[1] + ")";
    }

}
