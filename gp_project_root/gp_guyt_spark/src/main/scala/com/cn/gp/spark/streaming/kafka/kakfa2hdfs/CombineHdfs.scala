package com.cn.gp.spark.streaming.kafka.kakfa2hdfs

import com.cn.gp.hdfs.HdfsAdmin
import com.cn.gp.spark.common.SparkContextFactory
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{SQLContext, SaveMode}
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
    //  val sparkContext = SparkContextFactory.newSparkBatchContext("CombineHdfs")

    val sparkContext = SparkContextFactory.newSparkLocalBatchContext("CombineHdfs")
    //创建一个 sparkSQL
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    //遍历表 就是遍历HIVE表
    HiveConfig.tables.foreach(table => {
      //获取HDFS文件目录
      // /user/hive/warehouse/external/wechat
      val table_path = s"${HiveConfig.hive_root_path}$table"
      //通过sparkSQL 加载 这些目录的文件
      val tableDF = sqlContext.read.load(table_path)
      //先获取原来数据种的所有文件  HDFS文件 API
      val fileSystem: FileSystem = HdfsAdmin.getInstance().getFs
      //通过globStatus 获取目录下的正则匹配文件
      val arrayFileStatus = fileSystem.globStatus(new Path(table_path + "/part*"))
      //stat2Paths将文件状态转为文件路径   //这个文件路径是用来删除的
      val paths = FileUtil.stat2Paths(arrayFileStatus)
      //写入合并文件  //repartition 需要根据生产中实际情况去定义
      tableDF.repartition(1).write.mode(SaveMode.Append).parquet(table_path)
      println("写入" + table_path + "成功")
      //删除小文件
      paths.foreach(path => {
        HdfsAdmin.getInstance().getFs.delete(path)
        println("删除文件" + path + "成功")
      })

    })
  }
}
