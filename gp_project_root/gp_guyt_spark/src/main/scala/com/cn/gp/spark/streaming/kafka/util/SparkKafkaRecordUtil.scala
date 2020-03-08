package com.cn.gp.spark.streaming.kafka.util

import com.cn.gp.spark.common.CommonFields
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkKafkaRecordUtil extends Serializable {


  /**
    * @return org.apache.spark.streaming.dstream.DStream<scala.collection.immutable.Map<java.lang.String,java.lang.String>>
    * @author GuYongtao
    *         <p>从kafka消费消息</p>
    */
  def fromKafkaGetRecords(argsMap: java.util.Map[String, Object], ssc: StreamingContext
                         ): DStream[Map[String, String]] = {
    val kafkaParams = SparkKafkaConfigUtil.getKafkaParam(argsMap.get(CommonFields.BROKER_LIST).asInstanceOf[String],
      argsMap.get(CommonFields.GROUP_ID).asInstanceOf[String])
    // 获取KafkaInputDStream
    val kafkaInputDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](argsMap.get(CommonFields.TOPICS).asInstanceOf[Set[String]],
        kafkaParams))
    SparkUtil.convertInputDStream2DStreamMapObject(kafkaInputDStream)
  }

}
