package com.cn.gp.spark.streaming.kafka.kafka2hbase

import java.util

import com.cn.gp.common.config.ConfigUtil
import com.cn.gp.hbase.config.HBaseTableUtil
import com.cn.gp.hbase.extractor.{MapRowExtrator, SingleColumnMultiVersionRowExtrator}
import com.cn.gp.hbase.insert.HBaseInsertHelper
import com.cn.gp.hbase.search.{HBaseSearchService, HBaseSearchServiceImpl}
import com.cn.gp.hbase.spilt.SpiltRegionUtil
import com.cn.gp.spark.common.{CommonFields, SparkContextFactory}
import com.cn.gp.spark.streaming.kafka.kafka2es.Kafka2esStreaming.convertInputDStream2DStreamMapObject
import com.cn.gp.spark.streaming.kafka.{RunArgsUtil, SparkKafkaConfigUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * @return
  * @author GuYongtao
  *         <p>复杂版本</p>
  */
object ComplexDataRelationStreaming extends Serializable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(ComplexDataRelationStreaming.getClass)

  val complexRelationField = ConfigUtil.getInstance()
    .getProperties("spark/relation.properties")
    .get("complexRelationField")
    .toString
    .split(",")

  def main(args: Array[String]): Unit = {

    //初始化hbase表
    //nitRelationHbaseTable(complexRelationField)

    val ssc = SparkContextFactory.newSparkLocalStreamingContext("Complex-Data-Relation-Streaming",
      java.lang.Long.valueOf(30), 1)

    val argsMap = RunArgsUtil.argsCheckBrokerListGroupIdTopics(args)
    val kafkaParams = SparkKafkaConfigUtil.getKafkaParam(argsMap.get(CommonFields.BROKER_LIST).asInstanceOf[String],
      argsMap.get(CommonFields.GROUP_ID).asInstanceOf[String])
    // 获取KafkaInputDStream
    val kafkaInputDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](argsMap.get(CommonFields.TOPICS).asInstanceOf[Set[String]],
        kafkaParams))
    val kafkaDStream = convertInputDStream2DStreamMapObject(kafkaInputDStream)

    kafkaDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        while (partition.hasNext) {
          //双向关联
          val map = partition.next()
          //获取表名
          val table = map.get(CommonFields.TABLE_NAME).get
          //首先判断是不是特殊表(身份证信息)
          if (table.equals("card")) {
            val card = map.get("card").get
            val phone = map.get("phone").get
            if (StringUtils.isNotBlank(phone)) {
              //从索引表中去找手机号码，判断是
              var table: Table = null
              var exists = false
              //如果存在，则说明是总关联表先入库，则直接将phone 合并到总关联表钟
              try {
                table = HBaseTableUtil.getTable(s"${CommonFields.NAME_SPACE}:phone")
                exists = HBaseSearchServiceImpl.existsRowkey(table, phone)

                if (exists) {
                  //从索引表中获取总关联表的rowkey  获取phone对应的多版本 MAC
                  val baseSearchService: HBaseSearchService = new HBaseSearchServiceImpl()
                  val get = new Get(phone.getBytes)
                  get.setMaxVersions(100)
                  val set = new util.HashSet[String]()
                  val extractor = new SingleColumnMultiVersionRowExtrator(CommonFields.COLUMN_FAMILY.getBytes(), "phone_mac".getBytes(), set)
                  // 获取phone表下多版本的phone_mac
                  val macSet = baseSearchService.search(table.getName.getNameAsString, get, extractor)


                  macSet.foreach(macRowkey => {
                    // 插入主关联表
                    val put = new Put(macRowkey.getBytes())
                    val value = card
                    val versionNum = ("card" + value).hashCode() & Integer.MAX_VALUE
                    put.addColumn(CommonFields.COLUMN_FAMILY.getBytes(), Bytes.toBytes("card"), versionNum, Bytes.toBytes(value.toString))
                    HBaseInsertHelper.put(s"${CommonFields.NAME_SPACE}:relation", put)

                    //构建索引表
                    val put_2 = new Put(value.getBytes())
                    val table_name = s"${CommonFields.NAME_SPACE}:card"
                    //使用主表的rowkey 取hash作为二级索引的版本号
                    val versionNum_2 = macRowkey.hashCode() & Integer.MAX_VALUE
                    put_2.addColumn(CommonFields.COLUMN_FAMILY.getBytes(), Bytes.toBytes("phone_mac"), versionNum_2, Bytes.toBytes(macRowkey.toString))
                    HBaseInsertHelper.put(table_name, put_2)

                  })

                } else {
                  //如果不存在，则说明是总关联数据后入，需要关联进去的数据先入，
                  //将这条数据放入缓存表中
                  val put = new Put(phone.getBytes())
                  put.addColumn(CommonFields.COLUMN_FAMILY.getBytes(), Bytes.toBytes("card"), Bytes.toBytes(card.toString))
                  HBaseInsertHelper.put("cache:phone", put)
                }
              } catch {
                case e: Exception => LOGGER.error(null, e)
              } finally {
                HBaseTableUtil.close(table)
              }
            }
          } else {
            //如果不是特殊表，处理通用表
            val phone_mac: String = map.get("phone_mac").get
            //获取所有关联字段 //phone_mac,wechat,send_mail
            complexRelationField.foreach(relationField => {
              if (map.containsKey(relationField)) {
                //因为需要双向关联，所以这里也要通过phone取去查找缓存表中是否存在phone
                if ("phone".equals(relationField)) {
                  //判断该电话号码在缓存表中是否存在
                  val phone = map.get("phone").get
                  var table: Table = null
                  var exists = false
                  try {
                    table = HBaseTableUtil.getTable("cache:phone")
                    exists = HBaseSearchServiceImpl.existsRowkey(table, phone)
                  } catch {
                    case e: Exception => LOGGER.error(null, e)
                  } finally {
                    HBaseTableUtil.close(table)
                  }

                  //如果缓存表中存在这个电话号码，则将缓存中的身份取出来，写入到总关联表中
                  if (exists) {
                    val baseSearchServiceI: HBaseSearchService = new HBaseSearchServiceImpl()
                    val cacheMap = baseSearchServiceI.search("cache:phone", new Get(phone.getBytes()), new MapRowExtrator)
                    val card = cacheMap.get("card")

                    /** 写入数据到总关联表中，总关联表以MAC作为主键 **/
                    //  使用MAC作为主键
                    val put = new Put(phone_mac.getBytes())
                    //使用身份证作为值
                    val value = card
                    //使用"card" + 身份证号 作为版本号
                    val versionNum = ("card" + value).hashCode() & Integer.MAX_VALUE
                    put.addColumn(CommonFields.COLUMN_FAMILY.getBytes(), Bytes.toBytes("card"), versionNum, Bytes.toBytes(value.toString))
                    HBaseInsertHelper.put(s"${CommonFields.NAME_SPACE}:relation", put)
                  }
                }
                //主关联表
                val put = new Put(phone_mac.getBytes())
                val value = map.get(relationField).get
                val versionNum = (relationField + value).hashCode() & Integer.MAX_VALUE
                put.addColumn(CommonFields.COLUMN_FAMILY.getBytes(), Bytes.toBytes(relationField), versionNum, Bytes.toBytes(value.toString))
                println("put====" + put)
                HBaseInsertHelper.put(s"${CommonFields.NAME_SPACE}:relation", put)
                //建立二级索引
                //使用关联字段的值最为二级索引的rowkey
                val put_2 = new Put(value.getBytes())
                val table_name = s"${CommonFields.NAME_SPACE}:${relationField}"
                //使用主表的rowkey 取hash作为二级索引的版本号
                val versionNum_2 = phone_mac.hashCode() & Integer.MAX_VALUE
                put_2.addColumn(CommonFields.COLUMN_FAMILY.getBytes(), Bytes.toBytes("phone_mac"), versionNum_2, Bytes.toBytes(phone_mac.toString))
                HBaseInsertHelper.put(table_name, put_2)
              }
            })
          }
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * @return void
    * @author GuYongtao
    *         <p>创建表</p>
    */
  def initRelationHbaseTable(complexRelationField: Array[String]): Unit = {

    val relationTable = s"${CommonFields.NAME_SPACE}:relation"
    HBaseTableUtil.createTable(relationTable, CommonFields.COLUMN_FAMILY, true, -1, 100, SpiltRegionUtil.getSplitKeysBydinct)

    val cacheTable = CommonFields.CACHE_PHONE
    HBaseTableUtil.createTable(cacheTable, CommonFields.COLUMN_FAMILY, true, -1, 1, SpiltRegionUtil.getSplitKeysBydinct)

    DataRelationStreaming.relationFields.foreach(field => {
      val hbaseTable = s"${CommonFields.NAME_SPACE}:${field.trim}"
      HBaseTableUtil.createTable(hbaseTable, CommonFields.COLUMN_FAMILY, true, -1, 100, SpiltRegionUtil.getSplitKeysBydinct)
    })
  }

  /**
    * @return void
    * @author GuYongtao
    *         <p>删除表</p>
    */
  def deleteHbaseTable(relationFields: Array[String]): Unit = {
    val relationTable = s"${CommonFields.NAME_SPACE}:relation"
    HBaseTableUtil.deleteTable(relationTable)

    val cacheTable = CommonFields.CACHE_PHONE
    HBaseTableUtil.deleteTable(cacheTable)

    relationFields.foreach(field => {
      val hbaseTable = s"${CommonFields.NAME_SPACE}:${field}"
      HBaseTableUtil.deleteTable(hbaseTable)
    })
  }

}
