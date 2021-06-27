package com.feng.ml.clustering

import com.feng.ml.utils.{FileUtils, SparkUtils}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors


object KMeansCluster {
  def main(args: Array[String]): Unit = {
    val fileUtils = new FileUtils()
    val sc = SparkUtils.getSc
    val data = sc.textFile(fileUtils.getDataFile("kmeans_data_ext.txt"))
    val parsedData = data.map(x => Vectors.dense(x.split(" ", -1).map(_.toDouble))).cache()

    val initMode = KMeans.K_MEANS_PARALLEL // "k-means||"   "random"
    val numClusters = 4
    val numIterations = 20
    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(parsedData)

    val wsse = model.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $wsse")

    // 获取聚类好的聚类中心
    model.clusterCenters.foreach(println(_))

    val modelPath = fileUtils.getModelFile("kmeans")
    fileUtils.deleteFile(modelPath)
    model.save(sc, modelPath)

    val sameModel = KMeansModel.load(sc, modelPath)
    sameModel.clusterCenters.foreach(println(_))
  }

}
