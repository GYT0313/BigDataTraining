package com.cn.gp.spark.streaming.kafka.kafka2es

import com.alibaba.fastjson.TypeReference
import com.cn.gp.common.project.datatype.DataTypeProperties
import com.cn.gp.common.time.TimeTranstationUtils
import com.cn.gp.spark.common.SparkConfFactory
import com.cn.gp.spark.streaming.kafka.SparkKafkaConfigUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p>kafka数据写入es</p>
  */
object Kafka2esStreaming extends Serializable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(Kafka2esStreaming.getClass)

  // 数据类型wechat, search
  private val dataTypes: java.util.Set[String] = DataTypeProperties.dataTypeMap.keySet()

  def main(args: Array[String]): Unit = {
    //
    val topics = Set("gp_3")
    val groupId = "consumer-group-5"

    // 创建一个streaming context
    val ssc = SparkConfFactory.newSparkLocalStreamingContext("kafka2es", 5L)

    val kafkaParams = SparkKafkaConfigUtil.getKafkaParam("gp-guyt-1:9092,gp-guyt-2:9092", groupId)
    // 获取KakfaInputDStream
    val kafkaInputDSream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    val kafkaDStream = convertInputDStream2DStreamMapObject(kafkaInputDSream)
    // 增加index_date
    val newKafkaDStream = kafkaDStream.map(map => {
      map.put("index_date", TimeTranstationUtils.Date2yyyyMMddHHmmss(
        java.lang.Long.valueOf(map.get("collect_time") + "000")))
      map
    }).persist(StorageLevel.MEMORY_AND_DISK)

    print("入ES。。。")
    // 按数据类型入ES
    dataTypes.foreach(dataType => {
      val typeDS = newKafkaDStream.filter(x => {
        // x.get("table")得到是数据类型：wechat、qq，而不是x.get(dataType)
        dataType.equals(x.get("table"))
      })
      Kafka2esJob.insertData2EsByDate(dataType, typeDS)
    })
    val lines = kafkaDStream.map(_.values())
    lines.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  /**
    * @return org.apache.spark.streaming.dstream.DStream<java.util.Map<java.lang.String,java.lang.String>>
    * @author GuYongtao
    *         <p>将InputDStream转换为DStream</p>
    */
  def convertInputDStream2DStreamMapObject(kafkaDS: InputDStream[ConsumerRecord[String, String]]
                                          ): DStream[java.util.Map[String, String]] = {
    // 定义转换器
    val converter = { json: String => {
      var res: java.util.Map[String, String] = null
      try {
        // 转换为Map类型
        res = com.alibaba.fastjson.JSON.parseObject(json,
          new TypeReference[java.util.Map[String, String]]() {})
      } catch {
        case e: Exception => LOGGER.error(s"Kafka2ES 数据转换为Map失败: ", e)
      }
      res
    }
    }
    kafkaDS.map(x => converter(x.value().toString))
  }

}
