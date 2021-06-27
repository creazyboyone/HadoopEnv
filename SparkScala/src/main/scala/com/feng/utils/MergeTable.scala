package com.feng.utils

import org.apache.spark.sql.SparkSession

/**
 * 合并小文件 ORC
 * author: fqy
 */
object MergeTable {
  private val APP_NAME = "Hive Table Merge"

  def printDoc(): Unit = {
    println("===========================================")
    println("| Merge small table files to specify part |")
    println("| args1: table name ==> database.table    |")
    println("| args2: result file(s) number n >= 1     |")
    println("===========================================")
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "admin")
    // 参数检测
    if (args == null || args.length != 2) {
      println("[ERROR] Invalid params! you have " + args.length + " params.")
      for (i <- args) {
        println("[ERROR] params: " + i)
      }
      printDoc()
    } else if (!args(0).contains(".")) {
      // 第一个参数没有点，报错
      println("[ERROR] Table name must specify as => database.table")
      printDoc()
    } else {
      println("=== Start Spark " + APP_NAME + " Process ===")
      val sc = new SparkSession.Builder().appName(APP_NAME).enableHiveSupport.getOrCreate
      val data = sc.sql("SELECT * FROM " + args(0))
      data.coalesce(args(1).toInt).write.mode("overwrite").format("orc").saveAsTable(args(0) + "_merge")
      println("[INFO] === Process Success :) ===")
      println("[INFO] === Delete old table ===")
      sc.sql("DROP TABLE " + args(0))
      println("[INFO] === Rename tmp to New table ===")
      sc.sql("ALTER TABLE " + args(0) + "_merge RENAME TO " + args(0))
      println("[INFO] === Finish ===")
    }
  }
}
