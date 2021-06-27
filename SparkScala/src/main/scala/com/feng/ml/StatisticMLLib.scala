package com.feng.ml

import com.feng.ml.utils.{FileUtils, SparkUtils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics


object StatisticMLLib {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc
    val fileUtils = new FileUtils()
    val data = sc.textFile(fileUtils.getDataFile("sample_stat.txt")).map(x => x.split(" "))
    val data0 = data.map(f => f.map(f1 => f1.toDouble))
    val data1 = data0.map(f => Vectors.dense(f))
    val stat1 = Statistics.colStats(data1)
    println(stat1.max)
    println(stat1.min)
    println(stat1.mean)
    println(stat1.variance)
    println(stat1.normL1)
    println(stat1.normL2)

    val corr1 = Statistics.corr(data1, "pearson")
    val corr2 = Statistics.corr(data1, "spearman")
    val x1 = sc.parallelize(Array(1.0, 2.0, 3.0, 4.0))
    val y1 = sc.parallelize(Array(5.0, 6.0, 6.0, 6.0))
    val corr3 = Statistics.corr(x1, y1, "pearson")
    println("corr1=" + corr1)
    println("corr2=" + corr2)
    println("corr3=" + corr3)
  }

}
