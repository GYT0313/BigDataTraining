package com.cn.gp.spark.streaming.kafka

import java.util.concurrent.{ExecutorService, Executors}

import com.cn.gp.spark.streaming.kafka.kafka2es.Kafka2esStreaming
import com.cn.gp.spark.streaming.kafka.kafka2hbase.ComplexDataRelationStreaming
import com.cn.gp.spark.streaming.kafka.util.RunArgsUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p>Spark任务启动:
  *          1、Kafka 2 elasticsearch
  *          2、warning task
  *          3、kafka 2 hbase
  *          4、kafka 2 hive
  *          </p>
  */
object ApplicationSpark extends Serializable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(ApplicationSpark.getClass)

  def main(args: Array[String]): Unit = {
    val argsMap = RunArgsUtil.argsCheckBrokerListGroupIdTopics(args)

    // 线程池
    val threadPool: ExecutorService = Executors.newFixedThreadPool(5)

    val taskSList = new java.util.ArrayList[Runnable]()

    // 1、Kafka 2 elasticsearch
//    taskSList.add(new Kafka2esStreaming(argsMap))

    // 2、warning task

    // 3、kafka 2 hbase
    taskSList.add(new ComplexDataRelationStreaming(argsMap))

    // 4、kafka 2 hive

    // 执行线程
    try {
      taskSList.foreach(task => {
        threadPool.execute(task)
      })
    } catch {
      case e: Exception => LOGGER.error("执行线程错误", e)
    }

  }

}
