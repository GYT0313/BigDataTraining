package com.cn.gp.spark.streaming.kafka.kafka2hbase

import java.util

import com.cn.gp.hbase.insert.HBaseInsertHelper
import com.cn.gp.spark.common.CommonFields
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * @return
  * @author GuYongtao
  *         <p>插入数据</p>
  */
object Kafka2hbaseJob extends Serializable {

  protected final val LOGGER: Logger = LoggerFactory.getLogger(Kafka2hbaseJob.getClass)

  /**
    * @return void
    * @author GuYongtao
    *         <p>向HBase插入数据</p>
    */
  def insertHbase(wifiDS: DStream[Map[String, String]], hbase_table: String): Unit = {
    wifiDS.foreachRDD(rdd => {
      val putRDD = rdd.map(x => {
        val rowkey = x.get("id").get
        var put = new Put((rowkey.getBytes()))
        val keys = x.keySet
        keys.foreach(key => {
          put.addColumn(CommonFields.COLUMN_FAMILY.getBytes(), Bytes.toBytes(key), Bytes.toBytes(x.get(key).get))
        })
        put
      })

      putRDD.foreachPartition(partition => {
        var list = new util.ArrayList[Put]()
        while (partition.hasNext) {
          val put = partition.next()
          list.add(put)
        }
        HBaseInsertHelper.put(hbase_table, list, 10000)
        LOGGER.info("批量写入[" + hbase_table + ": " + list.size() + "]条数据")
      })

    })
  }
}
