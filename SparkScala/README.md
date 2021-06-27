# SparkWork
## Env
1. 下载安装Apache-Maven，本项目为maven工程，直接更新依赖（mvn clean package），即可运行。

|name|version|
|----|---|
|Java  |1.8|
|scala |2.13.4|
|spark |2.4.0-cdh6.2.1|
|hadoop|3.0.0-cdh6.2.1|

## Spark Submit 程序

大部分机器学习的查询参考来自[oboy](https://gitee.com/yaoboxu/sparkmllib)

### hive
1. [hive SQL的测试](src/main/scala/com/feng/hive/HiveTest.scala)

### ml(机器学习)
1. basic(基础算法)
  - IndexedRowMatrix
  - RowMatrix
  - VBreeze
2. classification(分类)
  - DecisionTreeC
  - NaiveBayes
  - SVM
3. clustering(聚类)
  - KMeansCluster

4. regression(回归)
  - LinearRegression
  - LogisticRegression

5. utils
  - FileUtils
  - LibSVMUtils
  - SparkUtils

6. 其他
  - BasicOperation
  - DataGenerate
  - Display
  - StatisticMLLib

### stream (Spark Streaming)
1. [SparkStreaming 接收 Kafka 存为 hive 表](src/main/scala/com/feng/stream/SparkStreamingV1.scala)

### Utils
1. [小文件合并](src/main/scala/com/feng/utils/MergeTable.scala)

### 其他
1. [WordCount 教程](src/main/scala/com/feng/WordCount.scala)
