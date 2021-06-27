package com.feng;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.Map;

/**
 * 类似max, min的聚合函数，但值是取第一个，不计算
 *
 * @author fqy
 * @since 2021/3/17
 */
@Description(name = "getOne",
        value = "Get the first value after Group by without calc anything",
        extended = "S = {5,12,3} max(S) = 12  min(S) = 3 getOne(S) = 5")
public class GetOne2 extends UDAF {

    public static class Evaluator implements UDAFEvaluator {
        private Map<String, String> courseScoreMap;

        @Override
        public void init() {

        }

        public boolean iterate(String course, String score) {
            if (course == null || score == null) {
                return true;
            }
            courseScoreMap.put(course, score);
            return true;
        }

        public Map<String, String> terminatePartial() {
            return courseScoreMap;
        }

        public boolean merge(Map<String, String> mapOutput) {
            this.courseScoreMap.putAll(mapOutput);
            return true;
        }

        public String terminate() {
            return courseScoreMap.toString();
        }
    }
}