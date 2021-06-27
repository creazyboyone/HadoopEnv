package com.feng.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Kafka => Spark streaming => Hive
 * author: fqy
 */
//noinspection DuplicatedCode
object SparkStreamingV1 {
  private val APP_NAME = "SparkStreaming 测试"
  // 微批时间间隔(时间窗口, 单位: 秒)
  private val duration = 5

  // Create Stream
  val schema: StructType = StructType(List(
    StructField("code", StringType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("address", StringType, nullable = true),
    StructField("income", StringType, nullable = true),
    StructField("time", StringType, nullable = true),
    StructField("experience", StringType, nullable = true),
    StructField("education", StringType, nullable = true),
    StructField("count", StringType, nullable = true),
    StructField("type", StringType, nullable = true),
    StructField("company", StringType, nullable = true),
    StructField("nature", StringType, nullable = true),
    StructField("scale", StringType, nullable = true),
    StructField("business", StringType, nullable = true),
    StructField("keyword", StringType, nullable = true),
    StructField("responsibility", StringType, nullable = true),
    StructField("requirement", StringType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    // env
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "admin")

    println("=== 启动SparkStreaming ===")
    // check args
    if (args == null || args.length != 0) {
      println("invalid params! ")
      for (i <- args) {
        println("params: " + i)
      }
    } else {
      // Create context
      val conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(duration.toLong))
      val hc = new SparkSession.Builder().appName(APP_NAME).enableHiveSupport().getOrCreate()

      ssc.checkpoint("/user/admin/checkpoint")

      // kafkaParams
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "slave2:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "testGroupID",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      // qc
      val qcTopics = Array("qc")
      val qcKafkaDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](qcTopics, kafkaParams))
      val qcSpyValue = qcKafkaDStream.map(_.value())
      qcSpyValue.foreachRDD(rdd => {
        if (rdd != null && rdd.count() != 0) {
          saveRDDToHive(hc, rdd, "test.qc_history")
        }
      })

      // lg
      val lgTopics = Array("lg")
      val lgKafkaDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](lgTopics, kafkaParams))
      val lgSpyValue = lgKafkaDStream.map(_.value())
      lgSpyValue.foreachRDD(rdd => {
        if (rdd != null && rdd.count() != 0) {
          saveRDDToHive(hc, rdd, "test.lg_history")
        }
      })

      // zl
      val zlTopics = Array("zl")
      val zlKafkaDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](zlTopics, kafkaParams))
      val zlSpyValue = zlKafkaDStream.map(_.value())
      zlSpyValue.foreachRDD(rdd => {
        if (rdd != null && rdd.count() != 0) {
          saveRDDToHive(hc, rdd, "test.zl_history")
        }
      })

      // tc
      val tcTopics = Array("tc")
      val tcKafkaDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](tcTopics, kafkaParams))
      val tcSpyValue = tcKafkaDStream.map(_.value())
      tcSpyValue.foreachRDD(rdd => {
        if (rdd != null && rdd.count() != 0) {
          saveRDDToHive(hc, rdd, "test.tc_history")
        }
      })

      println("===== 开始启动 ======")
      ssc.start()
      println("===== 开始等待数据 ======")
      ssc.awaitTermination()
      ssc.stop()
    }
  }

  def saveRDDToHive(hc: SparkSession, rdd: RDD[String], table: String): Unit = {
    import hc.implicits._
    val df = rdd.toDF("json")
    val res = df.withColumn("json", from_json(col("json"), schema)).select(
      col("json").getItem("code").as("code"),
      col("json").getItem("name").as("name"),
      col("json").getItem("address").as("address"),
      col("json").getItem("income").as("income"),
      col("json").getItem("time").as("time"),
      col("json").getItem("experience").as("experience"),
      col("json").getItem("education").as("education"),
      col("json").getItem("count").as("count"),
      col("json").getItem("type").as("type"),
      col("json").getItem("company").as("company"),
      col("json").getItem("nature").as("nature"),
      col("json").getItem("scale").as("scale"),
      col("json").getItem("business").as("business"),
      col("json").getItem("keyword").as("keyword"),
      col("json").getItem("responsibility").as("responsibility"),
      col("json").getItem("requirement").as("requirement")
    ).drop("json")
    res.write.format("orc").mode("append").saveAsTable(table)
  }
}
