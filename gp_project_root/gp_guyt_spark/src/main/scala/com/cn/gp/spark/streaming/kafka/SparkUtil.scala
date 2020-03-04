package com.cn.gp.spark.streaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.JavaConversions
import scala.util.parsing.json.JSON

object SparkUtil {


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

  def convertScalaMap2JavaMap(map: collection.mutable.Map[String, String]): java.util.Map[String, String] = {
    JavaConversions.mapAsJavaMap(map)
  }

}
