package com.feng.ml.classification

import com.feng.ml.utils.{FileUtils, SparkUtils}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.util.MLUtils

//noinspection DuplicatedCode
object SVM {
  def main(args: Array[String]): Unit = {
    val fileUtils = new FileUtils()
    val sc = SparkUtils.getSc
    val data = MLUtils.loadLibSVMFile(sc, fileUtils.getDataFile("sample_libsvm_data.txt"))
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val model = SVMWithSGD.train(training, 100)

    val predictAndLabel = test.map(x => {
      val score = model.predict(x.features)
      (score, x.label)
    })

    val print_predict = predictAndLabel.take(20)
    println("predict" + "\t" + "label")
    for (i <- print_predict.indices) {
      val tuple = print_predict(i)
      println(tuple._1 + "\t" + tuple._2)
    }

    //保证是float除法
    val accuracy = 100.0 * predictAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(s"accuracy = ${accuracy.formatted("%.2f")}%")

    val modelPath = fileUtils.getModelFile("svm")

    fileUtils.deleteFile(modelPath)

    model.save(sc, modelPath)
//    val sameModel = SVMModel.load(sc, modelPath)
//    sameModel.weights.foreachActive(x =>)
//    sameModel.pi.foreach(x => print(x + "\t"))
  }

}
