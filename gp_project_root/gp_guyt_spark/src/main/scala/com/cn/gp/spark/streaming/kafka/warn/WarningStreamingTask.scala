package com.cn.gp.spark.streaming.kafka.warn

import java.util.Timer

import com.cn.gp.redis.client.JedisSingle
import com.cn.gp.spark.common.SparkConfFactory
import com.cn.gp.spark.streaming.kafka.util.SparkKafkaConfigUtil
import com.cn.gp.spark.warn.service.BlackRuleWarning
import com.cn.gp.spark.warn.timer.SyncRule2Redis
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions
import scala.util.parsing.json.JSON

class WarningStreamingTask extends Serializable with Runnable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // 定义一个定时器，同步mysql到redis
    val timer: Timer = new Timer
    // 任务类: SyncRule2Redis, 0-启动时立即执行一次， 一分钟执行一次
    timer.schedule(new SyncRule2Redis, 0, 1 * 60 * 1000)

    // 从kafka获取数据流
    val topics = Set("gp_4")
    val groupId = "consumer-group-94"

    // 创建一个StreamingContext
    val ssc = SparkConfFactory.newSparkLocalStreamingContext("kafka2es", 5L)

    // 从kafka获取数据
    val kafkaParams = SparkKafkaConfigUtil.getKafkaParam("gp-guyt-1:9092,gp-guyt-2:9092,,gp-guyt-3:9092",
      groupId)
    val kafkaInputDSream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    val kafkaDStream = convertInputDStream2DStreamMapObject(kafkaInputDSream).persist(StorageLevel.MEMORY_AND_DISK)

    // 处理规则
    kafkaDStream.foreachRDD(rdd => {
      // 客户端连接不要放在RDD外面，因为处理partition时，数据需要分发到各个节点，数据分发需要序列化,如果不能序列化，将报错
      rdd.foreachPartition(partition => {
        var jedis: Jedis = null
        try {
          jedis = JedisSingle.getJedis(15)
          while (partition.hasNext) {
            val map = partition.next()
            // 强转
            val mapObject = JavaConversions.mapAsJavaMap(map)
            // 比较规则
            BlackRuleWarning.blackWarning(mapObject, jedis)
          }
        } catch {
          case e: Exception => LOGGER.error("预警错误", e)
        } finally {
          JedisSingle.close(jedis)
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()

  }


  def convertInputDStream2DStreamMapObject(kafkaDS: InputDStream[ConsumerRecord[String, String]]
                                          ): DStream[collection.immutable.Map[String, String]] = {
    // 定义转换器
    val converter = { json: String => {
      // 转换类型
      val res = JSON.parseFull(json) match {
        case Some(x: collection.immutable.Map[String, String]) => x
      }
      res
    }
    }

    kafkaDS.map(x => {
      converter(x.value().toString)
    })
  }

  override def run(): Unit = {

  }
}
