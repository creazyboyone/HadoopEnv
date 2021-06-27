package com.feng;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author fqy
 * @since 2021/3/2
 */

public class NewTest {
    private static final String APP_NAME = "Hive 测试";
    private static final Logger logger = LoggerFactory.getLogger(HiveTest.class);

    public static void main(String[] args) {

        logger.info("=== 启动Spark  ===");
//        SparkSession session = new SparkSession.Builder().appName(APP_NAME).enableHiveSupport().getOrCreate();
        JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName(APP_NAME));
//        SparkContext
        List<Tuple2<String, Integer>> tuple2List = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 90),
                new Tuple2<>("class1", 97),
                new Tuple2<>("class2", 89)
        );
        JavaPairRDD<String, Integer> rdd = context.parallelizePairs(tuple2List);
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = rdd.groupByKey();
        stringIterableJavaPairRDD.foreach((VoidFunction<Tuple2<String, Iterable<Integer>>>) t -> {
            System.out.println("class: " + t._1());
            for (Integer integer : t._2()) {
                System.out.println(integer);
            }
        });
        logger.info("=== 计算完成 ===");
//        session.close();
        context.close();
    }
}
