package com.cn.gp.spark.streaming.kafka.kakfa2hdfs

import java.util

import com.cn.gp.spark.common.CommonFields
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p></p>
  */
object CreateHiveTableTest extends Serializable {

  protected final val LOGGER: Logger = LoggerFactory.getLogger(CreateHiveTableTest.getClass)

  def main(args: Array[String]): Unit = {
    createHiveTable()
  }

  /**
    * @return void
    * @author GuYongtao
    *         <p>创建Hive外部表</p>
    */
  def createHiveTable(): Unit = {
    val sc = new SparkConf().setMaster("local[1]").setAppName("hive")
    val configuration: Configuration = new Configuration
    configuration.addResource(CommonFields.HIVE_SITE_XML)
    val iterator: util.Iterator[util.Map.Entry[String, String]] = configuration.iterator
    while ( {
      iterator.hasNext
    }) {
      val next: util.Map.Entry[String, String] = iterator.next
      sc.set(next.getKey, next.getValue)
    }
    sc.set("spark.sql.parquet.mergeSchema", "true")


    val spark = SparkSession
      .builder()
      .config(sc)
      .enableHiveSupport()
      .getOrCreate()
    val keys = HiveConfig.hiveTableSQL.keySet()
    //    spark.sql("use default")
    //    spark.sql("create table test2(id int)")
    keys.foreach(key => {
      val sql = HiveConfig.hiveTableSQL.get(key)
      //通过hiveContext 和已经创建好的SQL语句去创建HIVE表
      try {
        spark.sql(sql)
        println(s"创建表${key}成功")
      } catch {
        case e1: AlreadyExistsException => LOGGER.error(s"${key} 表已存在.", e1)
        case e2: Exception => LOGGER.error(s"${key} 表创建失败.", e2)
      }
    })
  }
}
