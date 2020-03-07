package com.cn.gp.spark.streaming.kafka.util

import com.cn.gp.spark.common.SparkConfFactory
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p>sparkstreaming 测试从kafka消费数据</p>
  */
object SparkStreamingKafkaTest extends Serializable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(SparkStreamingKafkaTest.getClass)

  def main(args: Array[String]): Unit = {
    val topics = Set("gp_3")
    val kafkaParam = SparkKafkaConfigUtil.getKafkaParam(
      "gp-guyt-1:9092,gp-guyt-2:9092,gp-guyt-3:9092", "consumer-group-96")
    val ssc = SparkConfFactory.newSparkLocalStreamingContext("sparkstreaming-test1", 5L)

    val kafkaDS = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))

    val lines = kafkaDS.map(_.value())
    lines.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
