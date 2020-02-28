package com.cn.gp.spark.streaming.kafka.kafka2es

import com.cn.gp.es.admin.AdminUtil
import com.cn.gp.es.client.ESClientUtils
import com.cn.gp.spark.common.convert.DataConvert
import com.cn.gp.spark.streaming.kafka.SparkEsConfigUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.LoggerFactory

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p>入ES</p>
  */
object Kafka2esJob extends Serializable {
  protected final val LOGGER = LoggerFactory.getLogger(Kafka2esJob.getClass)

  /**
    * @return
    * @author GuYongtao
    *         <p>按日期分组写入ES</p>
    */
  def insertData2EsByDate(dataType: String, typeDS: DStream[Map[String, String]], indexDateName: String): Unit = {
    // 通过 dateType(类型) + 日期(yyyyMMdd)  创建分片索引
    // 筛选时间，时间分组，不用groupBy（shuffle导致性能低，使用filter），一级过滤式dataType，二级过滤是日期
    val indexPrefix = dataType
    // es客户端
    val client: TransportClient = ESClientUtils.getClient

    // 把所有日期拿到，并去重
    typeDS.foreachRDD(rdd => {
      // 得到日期
      val days = getDays(rdd, indexDateName)
      // 使用日期对数据进行过滤，par 是scala的并发集合
      days.par.foreach(day => {
        // 前缀 + day组成索引, 如wecaht_20200226
        val index = indexPrefix + "_" + day
        // 判断es中是否存在该索引, 不存在则创建索引
        if (!AdminUtil.indexExists(client, index)) {
          val mappingPath = s"es/mapping/${indexPrefix}.json"
          AdminUtil.buildIndexAndTypes(index, index, mappingPath, 5, 1)
        }
        // 构建RDD，某一天的数据 tableRDD类型: RDD[Map[String, Object]]
        val tableRDD = rdd.filter(map => {
          day.equals(map.get(indexDateName).get)
        }).map(x => {
          // 把Map[String, String] 转为Map[String, Object]
          DataConvert.strMap2EsObjectMap(x)
        })
        // 写入ES
        EsSpark.saveToEs(tableRDD, dataType + "/" + dataType, SparkEsConfigUtil.getEsParam("id"))
      })
    })
  }


  /**
    * @return java.lang.String[]
    * @author GuYongtao
    *         <p>收集日期，去重(distinct)，集中到Driver(collect方法)</p>
    */
  def getDays(rdd: RDD[Map[String, String]], indexDateName: String): Array[String] = {
    rdd.map(x => {
      x.get(indexDateName).get
    }).distinct().collect()
  }


}
