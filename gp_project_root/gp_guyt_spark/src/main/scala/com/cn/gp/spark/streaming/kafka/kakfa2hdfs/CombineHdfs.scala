package com.cn.gp.spark.streaming.kafka.kakfa2hdfs

import com.cn.gp.hdfs.HdfsAdmin
import com.cn.gp.spark.common.{CommonFields, SparkConfFactory, SparkSessionFactory}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SaveMode
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p>合并HDFS小文件任务</p>
  */
object CombineHdfs extends Serializable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(HiveConfig.getClass)


  def main(args: Array[String]): Unit = {
    combineHadoopFile()
  }

  /**
    * @return void
    * @author GuYongtao
    *         <p>合并hadoop上hive外部表的小文件到一个大文件</p>
    */
  def combineHadoopFile(): Unit = {
    val spark = SparkSessionFactory.newSparkSession(SparkConfFactory.newSparkLocalConfHiveConf())

    // 遍历表 就是遍历HIVE表
    HiveConfig.tables.foreach(table => {
      // 获取HDFS文件目录
      // /user/hive/warehouse/external/wechat
      val tablePath = s"${CommonFields.HIVE_META_STORE_DIR}$table"
      // 通过sparkSQL 加载 这些目录的文件
      // 获取数据类型的schema 表结构, 并load数据
      val hadoopTablePath = s"hdfs://gp-guyt-1:8020${tablePath}"
      val tableDF = spark.read.schema(HiveConfig.mapSchema.get(table)).parquet(hadoopTablePath)

      // 先获取原来数据种的所有文件  HDFS文件 API
      val fileSystem: FileSystem = HdfsAdmin.getInstance().getFs
      // 通过globStatus 获取目录下的正则匹配文件
      val arrayFileStatus = fileSystem.globStatus(new Path(tablePath + "/part*"))
      // stat2Paths将文件状态转为文件路径   //这个文件路径是用来删除的
      val paths = FileUtil.stat2Paths(arrayFileStatus)
      // 写入合并文件  //repartition 需要根据生产中实际情况去定义
      try {
        tableDF.repartition(1).write.mode(SaveMode.Append).parquet(hadoopTablePath)
        // 删除小文件
        if (paths.nonEmpty) {
          paths.foreach(path => {
            HdfsAdmin.getInstance().getFs.delete(path)
          })
          println(s"[${table}]已合并...")
        } else {
          println(s"[${table}]无合并文件...")
        }
      } catch {
        case e: Exception => println(s"[${table}]合并失败...\n" + e)
      }



    })
  }
}
