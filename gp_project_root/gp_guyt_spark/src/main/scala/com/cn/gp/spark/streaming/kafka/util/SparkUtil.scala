package com.cn.gp.spark.streaming.kafka.util

import com.cn.gp.spark.common.CommonFields
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.JavaConversions
import scala.util.parsing.json.JSON

object SparkUtil extends Serializable {

  /**
    * @return org.apache.spark.streaming.dstream.DStream<scala.collection.immutable.Map<java.lang.String,java.lang.String>>
    * @author GuYongtao
    *         <p>将kafkaInputDStream转为DStream</p>
    */
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

  /**
    * @return java.util.Map<java.lang.String,java.lang.String>
    * @author GuYongtao
    *         <p>scala map  转为 java map</p>
    */
  def convertScalaMap2JavaMap(map: collection.mutable.Map[String, String]): java.util.Map[String, String] = {
    JavaConversions.mapAsJavaMap(map)
  }

  /**
    * @return java.util.Map<java.lang.String,java.lang.Object>
    * @author GuYongtao
    *         <p>将消费者组最后数字id更改</p>
    */
  def consumerGroupCumulative(argsMap: java.util.Map[String, Object],
                              cumulativeNum: Int): java.util.Map[String, Object] = {
    val newArgsMap = new java.util.HashMap[String, Object]
    newArgsMap.putAll(argsMap)
    val splits = String.valueOf(newArgsMap.get(CommonFields.GROUP_ID)).split("-")
    val newGroupNumString = (splits(1).toInt + cumulativeNum).toString
    newArgsMap.put(CommonFields.GROUP_ID, s"${splits(0)}-${newGroupNumString}")
    newArgsMap
  }


}
