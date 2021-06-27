package com.feng;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * 类似max, min的聚合函数，但值是取第一个，不计算
 *
 * @author fqy
 * @since 2021/3/17
 */
@Description(name = "getOne",
        value = "Get the first value after Group by without calc anything",
        extended = "S = {5,12,3} max(S) = 12  min(S) = 3 getOne(S) = 5")
public class GetOne extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
            throw new UDFArgumentTypeException(info.length - 1, "Exactly one argument is expected.");
        }
        ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info[0]);
        if (inspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Argument must be PRIMITIVE, but " + inspector.getCategory().name() + " was passed.");
        }
        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) inspector;
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "Argument must be String, but " + inputOI.getPrimitiveCategory().name() + " was passed.");
        }
        return new GetOneEvaluator();
    }

    public static class GetOneEvaluator extends GenericUDAFEvaluator {
        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        PrimitiveObjectInspector integerOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);

            //map阶段读取sql列，输入为String基础数据格式
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                //其余阶段，输入为Integer基础数据格式
                integerOI = (PrimitiveObjectInspector) parameters[0];
            }

            // 指定各个阶段输出数据格式都为Integer类型
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return outputOI;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return null;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {

        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {

        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return null;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {

        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            return null;
        }
    }
}