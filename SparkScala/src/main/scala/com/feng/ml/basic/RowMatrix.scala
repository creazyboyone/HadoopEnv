package com.feng.ml.basic

import com.feng.ml.Display
import com.feng.ml.utils.SparkUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix

//noinspection DuplicatedCode
object RowMatrix {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc
    val rdd1 = sc.parallelize(Array(Array(1.0, 2.0, 3.0, 4.0), Array(2.0, 3.0, 4.0, 5.0), Array(3.0, 4.0, 5.0, 6.0)))
      .map(f => Vectors.dense(f))
    rdd1.collect().foreach(x => println(x))

    //new 一个row matrix
    val RM = new RowMatrix(rdd1)

    //计算列之间的相关性
    //    val sim1 = RM.columnSimilarities(0.5)
    val sim2 = RM.columnSimilarities() //CoordinateMatrix(rowindex,colindex, values)
    sim2.entries.collect().foreach(println(_))
    println(sim2.numRows() + " x " + sim2.numCols())
    val rowsRdd = sim2.toRowMatrix().rows //先转成行矩阵->再转成稀疏的行向量
    rowsRdd.collect().foreach(f => println(f))

    //计算RowMatrix列的统计参数
    val sim3 = RM.computeColumnSummaryStatistics()
    println("max=" + sim3.max + " " + "min=" + sim3.min + "mean=" + sim3.mean)

    //计算行矩阵列之间的协方差
    val sim4 = RM.computeCovariance()
    Display.view(sim4)

    // PCA(主成分分析)
    val cc1 = RM.computePrincipalComponents(3)
    Display.view(cc1)

    //对于原始矩阵进行维度降维
    val dimenData = RM.multiply(cc1)

    //SVD分解   [U,S,V] = svd(A)
    val svd = RM.computeSVD(4, computeU = true)
    val u = svd.U //RowMatrix
    val s = svd.s //Vector
    val v = svd.V //Matrix
    u.rows.foreach(println(_))
    s.foreachActive((i, j) =>
      println(i, j))
    Display.view(v)

  }


}
