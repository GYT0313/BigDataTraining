package com.cn.gp.spark.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionFactory extends Serializable {

  def newSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
  }

  def newSparkSession(sc: SparkConf): SparkSession = {
    SparkSession
      .builder()
      .config(sc)
      .enableHiveSupport()
      .getOrCreate()
  }

}
