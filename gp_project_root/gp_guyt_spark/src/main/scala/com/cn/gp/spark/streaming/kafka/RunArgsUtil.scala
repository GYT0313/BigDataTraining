package com.cn.gp.spark.streaming.kafka

import com.cn.gp.spark.common.CommonFields
import scala.collection.JavaConversions._

object RunArgsUtil extends Serializable {


  /**
    * @return java.util.Map<java.lang.String,java.lang.Object>
    * @author GuYongtao
    *         <p></p>
    */
  def argsCheckMasterAppNameBrokerListGroupIdTopics(args: Array[String]): java.util.Map[String, Object] = {
    if (args.length < 3) {
      System.err.println("Usage: KafkaDirectWordCount <brokers> <groupId> <topics>")
      System.exit(1)
    }
    val Array(brokerList, group, topics) = args
    val map = new java.util.HashMap[String, Object]()
    map.put(CommonFields.BROKER_LIST, brokerList.mkString(","))
    map.put(CommonFields.GROUP_ID, group)
    map.put(CommonFields.TOPICS, Set(topics))
    map
  }

  /**
    * @return java.util.Map<java.lang.String,java.lang.Object>
    * @author GuYongtao
    *         <p>运行参数检测: brokers，groupid，topics</p>
    */
  def argsCheckBrokerListGroupIdTopics(args: Array[String]): java.util.Map[String, Object] = {
    if (args.length < 3) {
      System.err.println("Usage: KafkaDirectWordCount <brokers> <groupId> <topics>")
      System.exit(1)
    }
    val Array(brokerList, group, topics) = args
    val map = new java.util.HashMap[String, Object]()
    map.put(CommonFields.BROKER_LIST, brokerList.mkString)
    map.put(CommonFields.GROUP_ID, group)
    map.put(CommonFields.TOPICS, Set(topics))
    map
  }

  def main(args: Array[String]): Unit = {
    val map = argsCheckBrokerListGroupIdTopics(args)
    for ((k, v) <- map) {
      println(k)
      println(v)
    }
  }

}
