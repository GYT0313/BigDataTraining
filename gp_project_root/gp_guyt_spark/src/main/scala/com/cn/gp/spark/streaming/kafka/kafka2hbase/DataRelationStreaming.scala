package com.cn.gp.spark.streaming.kafka.kafka2hbase

import com.cn.gp.common.config.ConfigUtil
import com.cn.gp.hbase.config.HBaseTableUtil
import com.cn.gp.hbase.insert.HBaseInsertHelper
import com.cn.gp.hbase.spilt.SpiltRegionUtil
import com.cn.gp.spark.common.{CommonFields, SparkContextFactory}
import com.cn.gp.spark.streaming.kafka.kafka2es.Kafka2esStreaming.convertInputDStream2DStreamMapObject
import com.cn.gp.spark.streaming.kafka.{RunArgsUtil, SparkKafkaConfigUtil}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.JavaConversions._

/**
  * @return
  * @author GuYongtao
  *         <p>简单版本</p>
  */
object DataRelationStreaming extends Serializable {


  // 读取需要关联的配置文件字段
  // phone_mac,phone,username,send_mail,imei,imsi
  val relationFields = ConfigUtil.getInstance()
    .getProperties("spark/relation.properties")
    .get("relationField")
    .toString
    .split(",")

  def main(args: Array[String]): Unit = {
    val argsMap = RunArgsUtil.argsCheckBrokerListGroupIdTopics(args)

    //初始化hbase表
    // initRelationHbaseTable(relationFields)

    val ssc = SparkContextFactory.newSparkLocalStreamingContext("DataRelationStreaming", java.lang.Long.valueOf(30), 1)
    val kafkaParams = SparkKafkaConfigUtil.getKafkaParam(argsMap.get(CommonFields.BROKER_LIST).asInstanceOf[String],
      argsMap.get(CommonFields.GROUP_ID).asInstanceOf[String])
    // 获取KafkaInputDStream
    val kafkaInputDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](argsMap.get(CommonFields.TOPICS).asInstanceOf[Set[String]],
        kafkaParams))
    val kafkaDStream = convertInputDStream2DStreamMapObject(kafkaInputDStream)

    kafkaDStream.foreachRDD(rdd => {

      rdd.foreachPartition(partion => {
        //对partion进行遍历
        while (partion.hasNext) {

          //获取每一条流数据
          val map = partion.next()
          //获取mac 主键
          var phone_mac: String = map.get("phone_mac").get
          //获取所有关联字段 //phone_mac,phone,username,send_mail,imei,imsi
          relationFields.foreach(relationField => {
            //relationFields 是关联字段，需要进行关联处理的，所有判断
            // map中是不是包含这个字段，如果包含的话，取出来进行处理
            if (map.containsKey(relationField)) {
              //创建主关联  并遍历关联字段进行关联
              val put = new Put(phone_mac.getBytes())
              //取关联字段的值
              //TODO  到这里  主关联表的 主键和值都有了  然后封装成PUT写入hbase主关联表就行了
              val value = map.get(relationField).get
              //自定义版本号  通过 (表字段名 + 字段值 取hashCOde)
              //因为值有可能是字符串，但是版本号必须是long类型，所以这里我们需要
              //将字符串影射唯一数字，而且必须是正整数
              val versionNum = (relationField + value).hashCode() & Integer.MAX_VALUE
              put.addColumn("cf".getBytes(), Bytes.toBytes(relationField), versionNum, Bytes.toBytes(value.toString))
              HBaseInsertHelper.put("test:relation", put)
              println(s"往主关联表 test:relation 里面写入数据  rowkey=>${phone_mac} version=>${versionNum} 类型${relationField} value=>${value}")

              //建立二级索引
              //使用关联字段的值最为二级索引的rowkey
              // 二级索引就是把这个字段的值作为索引表rowkey,
              val put_2 = new Put(value.getBytes()) //把这个字段的值作为索引表rowkey,
              val table_name = s"test:${relationField}" //往索引表里面取写
              //使用主表的rowkey  就是 取hash作为二级索引的版本号
              val versionNum_2 = phone_mac.hashCode() & Integer.MAX_VALUE
              // 把这个字段的mac做为索引表的值
              put_2.addColumn("cf".getBytes(), Bytes.toBytes("phone_mac"), versionNum_2, Bytes.toBytes(phone_mac.toString))
              HBaseInsertHelper.put(table_name, put_2)
              println(s"往二级索表 ${table_name}里面写入数据  rowkey=>${value} version=>${versionNum_2} value=>${phone_mac}")
            }
          })
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }


  def initRelationHbaseTable(relationFields: Array[String]): Unit = {

    //初始化总关联表
    val relation_table = "test:relation"
    HBaseTableUtil.createTable(relation_table,
      "cf",
      true,
      -1,
      100,
      SpiltRegionUtil.getSplitKeysBydinct)
    //HBaseTableUtil.deleteTable(relation_table)

    //遍历所有关联字段，根据字段创建二级索引表
    relationFields.foreach(field => {
      val hbase_table = s"test:${field}"
      HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 100, SpiltRegionUtil.getSplitKeysBydinct)
      // HBaseTableUtil.deleteTable(hbase_table)
    })
  }


  def deleteHbaseTable(relationFields: Array[String]): Unit = {
    val relation_table = "test:relation"
    HBaseTableUtil.deleteTable(relation_table)
    relationFields.foreach(field => {
      val hbase_table = s"test:${field}"
      HBaseTableUtil.deleteTable(hbase_table)
    })
  }

}
