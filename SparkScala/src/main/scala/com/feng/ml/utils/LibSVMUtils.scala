package com.feng.ml.utils

import org.apache.spark.mllib.util.MLUtils

object LibSVMUtils {
  def main(args: Array[String]): Unit = {
    println(System.getProperty("user.dir"))
    val fileUtils = new FileUtils()
    val sc = SparkUtils.getSc
    val data = sc.textFile(fileUtils.getDataFile("sample_libsvm_data.txt"))
    val data_sample = MLUtils.loadLibSVMFile(sc, fileUtils.getDataFile("sample_libsvm_data.txt"))
    //rdd读取返回的是默认的原始数据，不方便使用，还需要我们自己去组装
    data.collect().take(1).foreach(println(_))
    //返回labelPoint数据格式(label,(n,indices, values))
    data_sample.take(1).foreach(println(_))

  }

}
