package com.cn.gp.spark.common

import java.io.IOException
import java.util
import java.util.Properties

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions


/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p>SparkConf工厂类</p>
  */
object SparkConfFactory extends Serializable {

  protected final val LOGGER: Logger = LoggerFactory.getLogger(SparkConfFactory.getClass)

  private val DEFAULT_BATCH_PATH = "/spark/spark-batch-config.properties"
  private val DEFAULT_STREAMING_PATH = "/spark/spark-streaming-config.properties"
  private val DEFAULT_STARTWITHJAVA_PATH = "/spark/spark-start-config.properties"


  def newSparkClusterConf(): SparkConf = {
    new SparkConf()
  }

  /**
    * @return org.apache.spark.SparkConf
    * @author GuYongtao
    *         <p>获取[本地]SparkConf</p>
    */
  def newSparkLocalConf(appName: String = "spark local", threads: Int = 2): SparkConf = {
    new SparkConf().setMaster(s"local[$threads]").setAppName(appName)
  }

  def newSparkConf(appName: String = "defualt"): SparkConf = {
    new SparkConf().setAppName(appName)
  }

  def newSparkLocalStreamingContext(appName: String = "sparkstreaming",
                                    batchInterval: Long = 30,
                                    threads: Int = 2): StreamingContext = {
    val sparkConf = SparkConfFactory.newSparkLocalConf(appName, threads)
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1")
    new StreamingContext(sparkConf, Seconds(batchInterval))
  }

  def newSparkClusterStreamingContext(): StreamingContext = {
    val sparkConf = SparkConfFactory.newSparkClusterConf()
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1")
    new StreamingContext(sparkConf, Seconds(30L))
  }


  def newSparkStreamingContext(appName: String = "sparkStreaming", batchInterval: Long = 30L): StreamingContext = {
    // batch interval多少秒读取一次
    val sparkConf = SparkConfFactory.newSparkConf(appName)
    new StreamingContext(sparkConf, Seconds(batchInterval))
  }


  /**
    * @return org.apache.spark.SparkConf
    * @author GuYongtao
    *         <p>SparkStreaming 集群处理 SparkConf</p>
    */
  def newSparkStreamingConf(appName: String = "defualt"): SparkConf = {
    val sparkConf = newSparkBatchConf(appName)
    sparkConf.setAll(readConfigFileAsTraversable(DEFAULT_STREAMING_PATH))
    sparkConf
  }

  /**
    * @return org.apache.spark.SparkConf
    * @author GuYongtao
    *         <p>离线[集群]批量处理</p>
    */
  def newSparkBatchConf(appName: String = "defualt"): SparkConf = {
    val sparkConf = newSparkConf(appName)
    sparkConf.setAll(readConfigFileAsTraversable(DEFAULT_BATCH_PATH))
    sparkConf
  }

  /**
    * @return org.apache.spark.SparkConf
    * @author GuYongtao
    *         <p>hive配置</p>
    */
  def newSparkLocalConfHiveConf(appName: String = "spark-hive", threads: Int = 2): SparkConf = {
    val sc = newSparkLocalConf(appName, threads)
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
    sc
  }


  def startSparkStreaming(ssc: StreamingContext): Unit = {
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  /**
    * @return scala.collection.Traversable<scala.Tuple2<java.lang.String,java.lang.String>>
    * @author GuYongtao
    *         <p>配置文件读取</p>
    */
  private def readConfigFileAsTraversable(path: String): Traversable[(String, String)] = {
    val prop = new Properties()
    val source = SparkConfFactory.getClass().getResourceAsStream(path)
    if (source == null) {
      LOGGER.error(s"未加载到 配置文件 $path 的数据...")
    } else {
      try {
        prop.load(source);
      } catch {
        case e: IOException => LOGGER.error(s"加载配置文件 $path 失败.", e);
      } finally {
        //将流关闭
        IOUtils.closeQuietly(source)
      }
    }
    // 转换为Scala类型
    val values = JavaConversions.collectionAsScalaIterable(prop.entrySet())
    val kvs = values.filter(map => {
      map.getValue != null
    }).map(map => {
      (map.getKey().toString().trim(), map.getValue().toString().trim())
    }).filter(!_._2.isEmpty())

    LOGGER.info(s"加载配置文件 $path 成功,具体参数如下：")
    LOGGER.info(kvs.toList.toString)
    kvs
  }
}
