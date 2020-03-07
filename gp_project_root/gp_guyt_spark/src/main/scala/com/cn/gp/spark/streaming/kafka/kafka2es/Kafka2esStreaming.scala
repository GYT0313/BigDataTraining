package com.cn.gp.spark.streaming.kafka.kafka2es

import com.cn.gp.common.project.datatype.DataTypeProperties
import com.cn.gp.common.time.TimeTranstationUtils
import com.cn.gp.spark.common.{CommonFields, SparkConfFactory}
import com.cn.gp.spark.streaming.kafka.util.{SparkKafkaRecordUtil, SparkUtil}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p>kafka数据写入es</p>
  */
class Kafka2esStreaming(val argsMap: java.util.Map[String, Object]) extends Serializable with Runnable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  // 数据类型wechat, search
  private val dataTypes: java.util.Set[String] = DataTypeProperties.dataTypeMap.keySet()

  /**
    * @return void
    * @author GuYongtao
    *         <p></p>
    */
  def fromKafka2ElasticSearch(argsMap: java.util.Map[String, Object]): Unit = {
    // 创建一个streaming context
    val ssc = SparkConfFactory.newSparkLocalStreamingContext("kafka_2_es-spark-task")
    val newArgsMap = SparkUtil.consumerGroupCumulative(argsMap, CommonFields.ID_ELASTIC_SEARCH)
    val kafkaDStream = SparkKafkaRecordUtil.fromKafkaGetRecords(newArgsMap, ssc)

    // 增加index_date
    val newKafkaDStream = kafkaDStream.map(map => {
      val collectTime = map.get(CommonFields.COLLECT_TIME) match {
        case Some(x) => x
      }
      var temp = map
      temp += (CommonFields.INDEX_DATE_NAME -> TimeTranstationUtils.Date2yyyyMMddHH(java.lang.Long.valueOf(collectTime + "000")))
      temp
    }).persist(StorageLevel.MEMORY_AND_DISK)

    // 按数据类型入ES
    dataTypes.foreach(dataType => {
      val typeDS = newKafkaDStream.filter(x => {
        // x.get("table")得到是数据类型：wechat、qq，而不是x.get(dataType)
        dataType.equals(x.get(CommonFields.TABLE_NAME).get)
      })
      Kafka2esJob.insertData2EsByDate(dataType, typeDS, CommonFields.INDEX_DATE_NAME)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  override def run(): Unit = {
    fromKafka2ElasticSearch(argsMap)
  }
}
