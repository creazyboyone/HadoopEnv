package com.feng;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;

/**
 * Spark(Java) 开发 Demo WordCount
 * @author fqy
 * @since 2020/10/10
 */

public class WordCount {
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) {
        // 检查参数的合法性
        if (args == null
                || args.length < 3
                || StringUtils.isBlank(args[0])
                || StringUtils.isBlank(args[1])
                || StringUtils.isBlank(args[2])) {
            logger.info("invalid params!");
        } else {

            String hdfsHost = args[0];
            String hdfsPort = args[1];
            String textFileName = args[2];

            logger.info("启动Spark任务");
            SparkConf sparkConf = new SparkConf().setAppName("Spark WordCount Application (java)");
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

            String hdfsBasePath = "hdfs://" + hdfsHost + ":" + hdfsPort + "/user/admin";
            String inputPath = hdfsBasePath + "/input/" + textFileName;
            String outputPath = hdfsBasePath + "/output/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

            logger.info("输入目录 ==> " + inputPath);
            logger.info("输出目录 <== " + outputPath);

            logger.info("text 装换 RDD");
            JavaRDD<String> textFile = javaSparkContext.textFile(inputPath);

            logger.info("Map 操作");
            JavaPairRDD<String, Integer> counts = textFile
                    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey(Integer::sum);

            logger.info("排序操作");
            JavaPairRDD<Integer, String> sorts = counts
                    .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()))
                    .sortByKey(false);

            logger.info("取前10");
            List<Tuple2<Integer, String>> top10 = sorts.take(10);
            StringBuilder sb = new StringBuilder("top 10 word :\n");
            for(Tuple2<Integer, String> tuple2 : top10) {
                sb.append(tuple2._2())
                        .append("\t")
                        .append(tuple2._1())
                        .append("\n");
            }
            logger.info(sb.toString());
            logger.info("合并保存文件");
            javaSparkContext.parallelize(top10).coalesce(1).saveAsTextFile(outputPath);

            logger.info("关闭上下文");
            //关闭context
            javaSparkContext.close();
        }
    }
}
