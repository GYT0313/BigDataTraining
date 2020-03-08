package com.cn.gp.spark.streaming.kafka.kafka2hbase

import com.cn.gp.spark.warn.service.BlackRuleWarning
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions

object EarlyWarningProess extends Serializable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  /**
    * @return void
    * @author GuYongtao
    *         <p></p>
    */
  def warningProcess(map: Map[String, String], jedis: Jedis): Unit = {
    // 强转
    val mapObject = JavaConversions.mapAsJavaMap(map)
    // 比较规则
    BlackRuleWarning.blackWarning(mapObject, jedis)
  }

}
