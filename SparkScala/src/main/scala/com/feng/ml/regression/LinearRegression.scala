package com.feng.ml.regression

import com.feng.ml.utils.{FileUtils, SparkUtils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}

//noinspection DuplicatedCode
object LinearRegression {
  def main(args: Array[String]): Unit = {
    val fileUtils = new FileUtils()
    val sc = SparkUtils.getSc
    val data = sc.textFile(fileUtils.getDataFile("lpsa.data"))
    val examples = data.map(line => {
      val parts = line.split(",", -1)
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ", -1).map(_.toDouble)))
    }).cache()

    val numExamples = examples.count()
    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 1.0
    val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
    val weight = model.weights
    val intercept = model.intercept
    println(s"model-weight=$weight, model-intercept=$intercept")

    val prediction = model.predict(examples.map(_.features))

    //将预测结果和原始标签，压缩成（predict,label）Tuple进行对比打印
    val predictionAndModel = prediction.zip(examples.map(_.label))
    val print_predict = predictionAndModel.take(50)
    for (i <- print_predict.indices) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    //误差平方
    val loss = predictionAndModel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)

    //计算均方根
    val rmse = math.sqrt(loss / numExamples)
    println(s"Test RMSE= $rmse.")

    //save model
    val modelPath = fileUtils.getModelFile("LinearR")

    fileUtils.deleteFile(modelPath)

    model.save(sc, modelPath)
    val sameModel = LinearRegressionModel.load(sc, modelPath)
    println(sameModel.weights)
  }

}
