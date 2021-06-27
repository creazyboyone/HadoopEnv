package com.feng.ml.classification

import com.feng.ml.utils.{FileUtils, SparkUtils}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

//noinspection DuplicatedCode
object NaiveBayes {
  def main(args: Array[String]): Unit = {
    val fileUtils = new FileUtils()
    val sc = SparkUtils.getSc
    val data = sc.textFile(fileUtils.getDataFile("sample_naive_bayes_data.txt"))
    val parseData = data.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ", -1).map(_.toDouble)))
    })

    val splits = parseData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    //建立模型
    val model = org.apache.spark.mllib.classification.NaiveBayes.train(
      training,
      lambda = 2.0,
      modelType = "multinomial")
    val predictionAndLabel = test.map(p => {
      (model.predict(p.features), p.label)
    })

    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- print_predict.indices) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(accuracy)

    val modelPath = fileUtils.getModelFile("bayes")

    fileUtils.deleteFile(modelPath)
    model.save(sc, modelPath)

    val sameModel = NaiveBayesModel.load(sc, modelPath)
    sameModel.pi.foreach(x => print(x + "\t"))
  }

}
