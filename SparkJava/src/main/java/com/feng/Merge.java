package com.feng;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author fqy
 * @since 2020/11/18
 */

public class Merge {
    private static final String APP_NAME = "Hive 12亿合并";
    private static final Logger logger = LoggerFactory.getLogger(Merge.class);

    public static void main(String[] args) {

        logger.info("=== 启动Spark任务 ===");

        SparkSession session = new SparkSession.Builder().appName(APP_NAME).enableHiveSupport().getOrCreate();

//        session.sql("SELECT * FROM xem.data_xem_testid_orc").groupBy("parent_id").agg(ConcatWs$(",", ));
                //.write().format("orc").mode(SaveMode.Overwrite).saveAsTable("xem.column_ejob_test");
        logger.info("=== 计算完成 ===");
        session.close();
    }
}
