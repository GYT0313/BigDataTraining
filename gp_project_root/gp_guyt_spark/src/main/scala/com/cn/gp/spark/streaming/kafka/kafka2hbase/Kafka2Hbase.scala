package com.cn.gp.spark.streaming.kafka.kafka2hbase

import java.util.Properties

import com.cn.gp.common.config.ConfigUtil
import com.cn.gp.hbase.config.HBaseTableUtil
import com.cn.gp.hbase.spilt.SpiltRegionUtil
import com.cn.gp.spark.common.{CommonFields, SparkContextFactory}
import com.cn.gp.spark.streaming.kafka.util.{RunArgsUtil, SparkKafkaConfigUtil, SparkUtil}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * @return
  * @author GuYongtao
  *         <p></p>
  */
object Kafka2Hbase extends Serializable {

  val kafkaConfig: Properties = ConfigUtil.getInstance().getProperties("kafka/kafka-server-config.properties")
  val topics = "chl_test8".split(",")

  def main(args: Array[String]): Unit = {
    val argsMap = RunArgsUtil.argsCheckBrokerListGroupIdTopics(args)

    val hbase_table = "test:chl_test8"
    HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 1, SpiltRegionUtil.getSplitKeysBydinct)


    val ssc = SparkContextFactory.newSparkLocalStreamingContext("DataRelationStreaming", java.lang.Long.valueOf(30), 1)
    val kafkaParams = SparkKafkaConfigUtil.getKafkaParam(argsMap.get(CommonFields.BROKER_LIST).asInstanceOf[String],
      argsMap.get(CommonFields.GROUP_ID).asInstanceOf[String])
    // 获取KafkaInputDStream
    val kafkaInputDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](argsMap.get(CommonFields.TOPICS).asInstanceOf[Set[String]],
        kafkaParams))
    val kafkaDStream = SparkUtil.convertInputDStream2DStreamMapObject(kafkaInputDStream)

    Kafka2hbaseJob.insertHbase(kafkaDStream, hbase_table)

    ssc.start()
    ssc.awaitTermination()
  }
}
