package com.feng.ml.basic

import com.feng.ml.utils.SparkUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

//noinspection DuplicatedCode
object IndexedRowMatrix {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc
    val rdd1 = sc.parallelize(Array(Array(1.0, 2.0, 3.0, 4.0), Array(2.0, 3.0, 4.0, 5.0), Array(3.0, 4.0, 5.0, 6.0)))
      .map(f => IndexedRow(f(0).toInt, Vectors.dense(f)))
    rdd1.collect().foreach(println(_))
    val indexedRowMatrix = new IndexedRowMatrix(rdd1)
    indexedRowMatrix.rows.foreach(println(_))

    val blockMatrix = indexedRowMatrix.toBlockMatrix(1, 1)
    blockMatrix.blocks.collect().foreach(println(_))
  }


}
