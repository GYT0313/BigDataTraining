package com.cn.gp.spark.streaming.kafka.kakfa2hdfs

import java.util.Timer

import com.cn.gp.hdfs.HdfsAdmin
import com.cn.gp.spark.common.{CommonFields, SparkConfFactory, SparkSessionFactory}
import com.cn.gp.spark.streaming.kafka.util.{RunArgsUtil, SparkKafkaRecordUtil, SparkUtil}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object Kafka2HiveStreaming extends Serializable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(Kafka2HiveStreaming.getClass)


  /**
    * @return void
    * @author GuYongtao
    *         <p>kafka入hive</p>
    */
  def fromKafka2Hive(argsMap: java.util.Map[String, Object]): Unit = {
    val ssc = SparkConfFactory.newSparkLocalStreamingContext()
    val kafkaDStream = SparkKafkaRecordUtil.fromKafkaGetRecords(argsMap, ssc)

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
          val tableDF = SparkUtil.rdd2DF(rdd, schemaFields, spark, schema)
          if (!tableDF.isEmpty) {
            //多种数据一起处理
            val pathAll = s"${CommonFields.HADOOP_HTTP_URL}${CommonFields.HIVE_META_STORE_DIR}${table}"
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

  def synchronizeCombineHadoopFile(): Unit = {
    // 定义一个定时器，同步mysql到redis
    val timer: Timer = new Timer
    // 任务类: SyncRule2Redis, 启动3分钟后立即执行一次， 5分钟执行一次
    timer.schedule(new CombinHadoop, 1 * 1000 * 60 * 3, 1 * 1000 * 60 * 3)
  }

  def main(args: Array[String]): Unit = {
    synchronizeCombineHadoopFile()
    val argsMap = RunArgsUtil.argsCheckBrokerListGroupIdTopics(args)
    fromKafka2Hive(argsMap)
  }
}
