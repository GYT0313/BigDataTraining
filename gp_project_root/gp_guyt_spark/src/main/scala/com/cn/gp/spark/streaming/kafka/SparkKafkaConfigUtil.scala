package com.cn.gp.spark.streaming.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p>Kafka参数</p>
  */
object SparkKafkaConfigUtil extends Serializable {
  def getKafkaParam(brokerList: String, groupId: String): Map[String, Object] = {
    val kafkaParam = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
    //    val kafkaParam = Map[String, Object](
    //      "metadata.broker.list" -> brokerList,
    //      "auto.offset.reset" -> "earliest",
    //      "group.id" -> groupId,
    //      "refresh.leader.backoff.ms" -> "1000",
    //      "num.consumer.fetchers" -> "8",
    //      "key.deserializer" -> "org.apache.kafka.common.serialization.Deserializer<T>",
    //      "value.deserializer" -> "org.apache.kafka.common.serialization.Deserializer<T>"
    //    )
    kafkaParam
  }

}
