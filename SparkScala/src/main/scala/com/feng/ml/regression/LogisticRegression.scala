package com.feng.ml.regression

import com.feng.ml.utils.{FileUtils, SparkUtils}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

//noinspection DuplicatedCode
object LogisticRegression {
  def main(args: Array[String]): Unit = {
    val fileUtils = new FileUtils()
    val sc = SparkUtils.getSc

    // 读取数据，并拆分成训练和测试数据
    val data = MLUtils.loadLibSVMFile(sc, fileUtils.getDataFile("sample_libsvm_data.txt"))
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // 模型训练
    //    val model = LogisticRegressionWithSGD.train(training,1000, 1, 1.0)   // miniBatchGradientDescent
    val model = new LogisticRegressionWithLBFGS().setNumClasses(10).run(training) // BFGS


    // 测试模型
    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    val print_predict = predictionAndLabels.take(20)
    println("prediction" + "\t" + "label")
    for (i <- print_predict.indices) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    // 误差计算
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println(s"Precision = $precision")

    //保存模型
    val modelPath = fileUtils.getModelFile("LR")

    //历史训练模型和元数据的清理

    fileUtils.deleteFile(modelPath)

    model.save(sc, modelPath)

    //加载模型
    val sameModel = LogisticRegressionModel.load(sc, modelPath)
    println(sameModel.weights)
  }

}
