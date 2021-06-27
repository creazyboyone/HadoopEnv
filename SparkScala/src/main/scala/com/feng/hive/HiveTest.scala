package com.feng.hive

import org.apache.spark.sql.SparkSession

/**
 * hive查询测试
 * author: fqy
 */

object HiveTest {
  private val APP_NAME = "Hive 测试"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "admin")
    println("=== 启动Spark任务 ===")
    if (args == null || args.length != 0) {
      println("invalid params! ")
      for (i <- args) {
        println("params: " + i)
      }
    } else {
      val sc = new SparkSession.Builder().appName(APP_NAME).enableHiveSupport.getOrCreate
      val res = sc.sql("SELECT * FROM xem.column_job").count()
      println("====[ " + res + " ]=====")
      println("=== 计算完成 ===")
      sc.close()
    }
  }
}
