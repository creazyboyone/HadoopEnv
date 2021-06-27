package com.feng.ml

import com.feng.ml.utils.SparkUtils
import org.apache.spark.mllib.util.{KMeansDataGenerator, LinearDataGenerator, LogisticRegressionDataGenerator}


object DataGenerate {
  def main(args: Array[String]): Unit = {
    //K-means数据构造器
    val sc = SparkUtils.getSc
    val KMeansRdd = KMeansDataGenerator.generateKMeansRDD(sc,40,3,3,1.0, 2)
    println(KMeansRdd.count())
    KMeansRdd.take(1).foreach(x=>x.foreach(println(_)))

    //Linear regression线性回归数据构造器
    val linearRdd = LinearDataGenerator.generateLinearRDD(sc,40,3,1.0,2,0.0)
    linearRdd.take(1).foreach(println(_))

    //Logistic Regression逻辑回归数据构造器
    val logisticRdd = LogisticRegressionDataGenerator.generateLogisticRDD(sc,40,3,1.0,2)
    logisticRdd.take(5).foreach(println(_))

  }

}
