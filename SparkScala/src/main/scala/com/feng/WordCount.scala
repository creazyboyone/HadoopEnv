package com.feng

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    print("HelloWord")
    System.setProperty("hadoop.home.dir", "D:\\jdk\\hadoop-2.7.6")
    val resourcesPath = "src/main/resources/"

    val sparkConf = new SparkConf().setMaster("local").setAppName("sparkDemo")
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(resourcesPath + "word.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // delete if files exist
    val file = new File(resourcesPath + "word_result")
    if (file.exists()) {
      val listFiles = file.listFiles()
      listFiles.foreach(_.delete())
      file.delete()
    }
    counts.saveAsTextFile(resourcesPath + "word_result")
  }
}