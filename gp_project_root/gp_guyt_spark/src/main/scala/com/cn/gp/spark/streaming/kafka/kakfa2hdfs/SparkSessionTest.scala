package com.cn.gp.spark.streaming.kafka.kakfa2hdfs

import com.cn.gp.spark.common.{SparkConfFactory, SparkSessionFactory}

object SparkSessionTest extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionFactory.newSparkSession(SparkConfFactory.newSparkLocalConfHiveConf())
    spark.sql("use default")
    spark.sql("select * from wechat").show()
  }
}
