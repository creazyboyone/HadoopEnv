package com.feng.ml.basic

import breeze.linalg.{Axis, DenseMatrix, DenseVector, argmax, diag, sum, svd, trace}
import breeze.math.Complex
import breeze.numerics.{constants, exp, log10}

object VBreeze {
  def main(args: Array[String]): Unit = {
    println(DenseVector.rand(4))
    println(DenseMatrix.zeros[Double](4, 5)) //全零矩阵
    println(diag(DenseVector(1.0, 2.0, 3.0))) //对角矩阵
    val randMatrix = DenseMatrix.rand[Double](2, 3)
    println(randMatrix) //随机矩阵
    println("转置矩阵:")
    println(randMatrix.t)
    println(DenseMatrix(Array((1, 2, 3), (4, 5, 6)))) //无法根据数据数组创建矩阵（X）
    println(DenseMatrix((1, 2, 3), (4, 5, 6)))

    val a = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
//    val b = DenseMatrix((1.0, 1.0, 1.0), (2.0, 2.0, 2.0))
    println(a)
    //    println(b)
    //    println(a dot b)  //只有矩阵有点积
    //向量点积
    println(DenseVector(1, 2, 3, 4) dot DenseVector(1, 1, 1, 1))
    println(argmax(a))

    println(sum(a, Axis._0))
    println(sum(a, Axis._1))

    val a_square = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0), (7.0, 8.0, 9.0))
    println("方阵：\n" + a_square)
    println("对角线元素之和：" + trace(a_square))
    println(a.size)

    val svd.SVD(u, s, v) = svd(a) //奇异值分解
    println("u=\n" + u + "\n" + "s=\n" + s + "\n" + "v=\n" + v)

    println("指数函数=" + constants.E)
    println("PI=" + constants.Pi)
    println("π=" + constants.π)

    //复数操作
    val z = Complex(3, 4)
    println("实数部分：" + z.real)
    println("虚数部分：" + z.im())
    println("z的共轭复数：" + z.conjugate)
    println("对数log_10(10)=" + log10(10))
    println("指数exp10=" + exp(2.0))
  }


}
