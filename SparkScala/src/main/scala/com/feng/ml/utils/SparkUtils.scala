package com.feng.ml.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSc: SparkContext = {
    val sparkSession = SparkSession.builder()
      .appName("ml_test")
      .master("local[*]")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc
  }

}
