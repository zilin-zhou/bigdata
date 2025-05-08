package hive.study;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class Mean extends UDAF {
    // 定义UDAF的Evaluator类
    public static class MeanDoubleUDAFEvaluator implements UDAFEvaluator {
        public static class PartialResult {
            double sum;
            long count;
        }
        private PartialResult partial;
        // 初始化聚合函数的状态
        public void init() {
            partial = null;
        }
        // 聚合函数的迭代逻辑
        public boolean iterate(Double value) {
            if (value == null) {
                return true;
            }
            if (partial == null) {
                partial = new PartialResult();
            }
            partial.sum += value;
            partial.count++;
            return true;
        }
        // 返回聚合函数的部分结果
        public PartialResult terminatePartial() {
            return partial;
        }
    // 合并两个部分结果
        public boolean merge(PartialResult other) {
            if (other == null) {
                return true;
            }
            if (partial == null) {
                partial = new PartialResult();
            }
            partial.sum += other.sum;
            partial.count += other.count;
            return true;
        }
// 返回最终的聚合结果

        public Double terminate() {
            if (partial == null) {
                return null;
            }
            return partial.sum / partial.count;
        }
    }
}
