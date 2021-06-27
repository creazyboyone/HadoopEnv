package com.feng;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author fqy
 * @since 2020/11/18
 */

public class HiveTest {
    private static final String APP_NAME = "Hive 测试";
    private static final Logger logger = LoggerFactory.getLogger(HiveTest.class);

    public static void main(String[] args) {

        logger.info("=== 启动Spark任务 ===");

        // 检查参数的合法性
        if (args == null
                || args.length < 1
                || StringUtils.isBlank(args[0])) {
            logger.info("invalid params!");
        } else {
            SparkSession session = new SparkSession.Builder().appName(APP_NAME).enableHiveSupport().getOrCreate();
            session.sql("SELECT * FROM test")
                    .filter("id > " + args[0])
                    .write().format("orc").mode(SaveMode.Overwrite).saveAsTable("test1");
            logger.info("=== 计算完成 ===");
            session.close();
        }
    }
}
