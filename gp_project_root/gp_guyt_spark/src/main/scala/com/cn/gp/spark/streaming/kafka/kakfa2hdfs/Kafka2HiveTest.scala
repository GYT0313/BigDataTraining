package com.cn.gp.spark.streaming.kafka.kakfa2hdfs

import java.util

import com.cn.gp.hdfs.HdfsAdmin
import com.cn.gp.spark.common.{CommonFields, SparkConfFactory, SparkSessionFactory}
import com.cn.gp.spark.streaming.kafka.{RunArgsUtil, SparkKafkaConfigUtil}
import com.cn.gp.spark.streaming.kafka.kafka2es.Kafka2esStreaming.convertInputDStream2DStreamMapObject
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p></p>
  */
object Kafka2HiveTest extends Serializable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(Kafka2HiveTest.getClass)

  def main(args: Array[String]): Unit = {
    val argsMap = RunArgsUtil.argsCheckBrokerListGroupIdTopics(args)
    fromKafka2Hive(argsMap)
  }

  /**
    * @return void
    * @author GuYongtao
    *         <p>kafka入hive</p>
    */
  def fromKafka2Hive(argsMap: java.util.Map[String, Object]): Unit = {
    // 创建一个streaming context
    val ssc = SparkConfFactory.newSparkLocalStreamingContext("spark-streaming-hive", 20L)

    val kafkaParams = SparkKafkaConfigUtil.getKafkaParam(argsMap.get(CommonFields.BROKER_LIST).asInstanceOf[String],
      argsMap.get(CommonFields.GROUP_ID).asInstanceOf[String])
    // 获取KafkaInputDStream
    val kafkaInputDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](argsMap.get(CommonFields.TOPICS).asInstanceOf[Set[String]],
        kafkaParams))
    val kafkaDStream = convertInputDStream2DStreamMapObject(kafkaInputDStream)


    val lines = kafkaInputDStream.map(_.value())
    lines.print()

    val spark = SparkSessionFactory.newSparkSession(SparkConfFactory.newSparkLocalConfHiveConf())

    HiveConfig.tables.foreach(table => {
      //过滤出单一数据类型(获取和table相同类型的所有数据)
      val tableDS = kafkaDStream.filter(x => {
        table.equals(x.get(CommonFields.TABLE_NAME).get)
      })
      //获取数据类型的schema 表结构
      val schema = HiveConfig.mapSchema.get(table)
      //获取这个表的所有字段
      val schemaFields: Array[String] = schema.fieldNames
      tableDS.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          //TODO 数据写入HDFS
          /*        val sc = rdd.sparkContext
                  val hiveContext = HiveConf.getHiveContext(sc)
                  hiveContext.sql(s"USE DEFAULT")*/
          //将RDD转为DF   原因：要加字段描述，写比较方便 DF
          val tableDF = rdd2DF(rdd, schemaFields, spark, schema)
          if (!tableDF.isEmpty) {
            //多种数据一起处理
            val pathAll = s"hdfs://gp-guyt-1:8020${CommonFields.HIVE_META_STORE_DIR}${table}"
            val exists = HdfsAdmin.getInstance().getFs.exists(new Path(pathAll))

            //2.写到HDFS   不管存不存在都要把数据写入进去 通过追加的方式
            //每10秒写一次，写一次会生成一个文件
            tableDF.write.mode(SaveMode.Append).parquet(pathAll)

            //3.加载数据到HIVE
            if (!exists) {
              //如果不存在 进行首次加载
              System.out.println("===================开始加载数据到分区=============")
              spark.sql(s"ALTER TABLE ${table} LOCATION '${pathAll}'")
            }
          }
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    * @author GuYongtao
    *         <p>将RDD转为DF</p>
    */
  def rdd2DF(rdd: RDD[Map[String, String]],
             schemaFields: Array[String],
             spark: SparkSession,
             schema: StructType): DataFrame = {

    //将RDD[Map[String,String]]转为RDD[ROW]
    val rddRow = rdd.map(record => {
      val listRow: util.ArrayList[Object] = new util.ArrayList[Object]()
      for (schemaField <- schemaFields) {
        listRow.add(record.get(schemaField).get)
      }
      Row.fromSeq(listRow)
    }).repartition(1)
    //构建DF
    //def createDataFrame(rowRDD: RDD[Row], schema: StructType)
    val typeDF = spark.createDataFrame(rddRow, schema)
    typeDF
  }

}
