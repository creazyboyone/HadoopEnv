
package com.feng.ml

import com.feng.ml.utils.SparkUtils

object BasicOperation {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSc
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = rdd1.map(x => x * 2)
    rdd2.collect().foreach(println(_))

    val rdd3 = rdd2.filter(x => x > 10)
    rdd3.collect().foreach(println(_))

    val rdd4 = rdd3.flatMap(x => x to 20)
    rdd4.collect().foreach(println(_))


    val rdd4_1 = rdd3.map(x => x to 20)
    rdd4_1.collect().foreach(println(_))
    //    输出如下，也是每个数都增长都了20，但是每一个数都是独立，没有想flatmap最后融合成一个Array或者Sequence
    //    Range(12, 13, 14, 15, 16, 17, 18, 19, 20)
    //    Range(14, 15, 16, 17, 18, 19, 20)
    //    Range(16, 17, 18, 19, 20)
    //    Range(18, 19, 20)

    val rdd5 = rdd1.mapPartitions(myFun)
    rdd5.collect().foreach(println(_))
    rdd5.groupByKey()

  }

  def myFun[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next()
    while (iter.hasNext) {
      val cur = iter.next()
      println((pre, cur))
      res.::=(pre, cur)
      pre = cur
    }
    res.iterator
  }

}
