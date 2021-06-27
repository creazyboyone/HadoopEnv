
package com.feng.ml

import org.apache.spark.mllib.linalg.Matrix

object Display {
  def view(matrix: Matrix): Unit = {
    for (i <- 0 until matrix.numRows) {
      for (j <- 0 until matrix.numCols) {
        print(matrix.apply(i, j))
        print("\t")
      }
      print("\n")
    }
  }
}
