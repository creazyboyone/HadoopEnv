package com.feng.ml.classification


import com.feng.ml.utils.{FileUtils, SparkUtils}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils


//noinspection DuplicatedCode
object DecisionTreeC {
  def main(args: Array[String]): Unit = {
    val fileUtils = new FileUtils()
    val sc = SparkUtils.getSc
    val data = MLUtils.loadLibSVMFile(sc, fileUtils.getDataFile("sample_libsvm_data.txt"))
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val tests = splits(1)

    //
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(training,
      numClasses,
      categoricalFeaturesInfo,
      impurity,
      maxDepth,
      maxBins)

    val labelAndPredicts = tests.map(x => {
      val predict = model.predict(x.features)
      (x.label, predict)
    })

    val printPredict = labelAndPredicts.take(20)
    println("label" + "\t" + "predict")
    for (i <- printPredict.indices) {
      println(printPredict(i)._1 + "\t" + printPredict(i)._2)
    }

    //计算准确率
    val accuracy = 1.0 * labelAndPredicts.filter(x => x._1 == x._2).count() / tests.count()

    println(s"accuracy is: $accuracy")
    println("Learned classification model is:\n" + model.toDebugString)

    //保存模型
    val modelPath = fileUtils.getModelFile("decision_tree")

    fileUtils.deleteFile(modelPath)
    model.save(sc, modelPath)

    val sameModel = DecisionTreeModel.load(sc, modelPath)
    println(sameModel.toDebugString)
  }

}
