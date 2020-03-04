package com.cn.gp.spark.streaming.kafka.kakfa2hdfs

import java.util

import org.apache.commons.configuration.{CompositeConfiguration, ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object HiveConfig extends Serializable {
  protected final val LOGGER: Logger = LoggerFactory.getLogger(HiveConfig.getClass)
  //HIVE 文件根目录
  //  var hive_root_path = "/user/kf_xy_xy_mn/del_cache_table/"
  var hive_root_path = "/user/hive/warehouse/external/"
  var hiveFieldPath = "es/mapping/field-mapping.properties"

  var config: CompositeConfiguration = null
  //所有的表
  var tables: util.List[_] = null
  //表对应所有的字段映射,可以通过table名获取 这个table的所有字段，格式如[wechat={wechat.phone=string,...},...]
  var tableFieldsMap: util.Map[String, util.HashMap[String, String]] = null
  //StructType
  var mapSchema: util.Map[String, StructType] = null
  //建表语句
  var hiveTableSQL: util.Map[String, String] = null

  /**
    * 主要就是创建mapSchema  和  hiveTableSQL
    * 初始化
    */
  initParams()


  /**
    * @return void
    * @author GuYongtao
    *         <p>初始化HIVE参数</p>
    */
  def initParams(): Unit = {
    //加载es/mapping/fieldmapping.properties 配置文件
    config = HiveConfig.readCompositeConfiguration(hiveFieldPath)
    println("==========================config====================================")
    config.getKeys.foreach(key => {
      // key=等号左边，config.getProperty(key.toString)=等号右边
      println(key + ":" + config.getProperty(key.toString))
    })
    println("==========================tables====================================")
    //wechat, search
    tables = config.getList("tables")
    tables.foreach(table => {
      println(table)
    })
    println("======================tableFieldsMap================================")
    //(qq,{qq.imsi=string, qq.id=string, qq.send_message=string, qq.filename=string})
    tableFieldsMap = HiveConfig.getKeysByType()
    tableFieldsMap.foreach(x => {
      println(x)
    })
    println("=========================mapSchema===================================")
    mapSchema = HiveConfig.createSchema()
    mapSchema.foreach(x => {
      println(x)
    })
    println("=========================hiveTableSQL===================================")
    hiveTableSQL = HiveConfig.getHiveTables()
    hiveTableSQL.foreach(x => {
      println(x)
    })
  }

  /**
    * @return org.apache.commons.configuration.CompositeConfiguration
    * @author GuYongtao
    *         <p>读取hive 字段配置文件</p>
    */
  def readCompositeConfiguration(path: String): CompositeConfiguration = {
    LOGGER.info("加载配置文件: " + path)
    //多配置工具
    val compositeConfiguration = new CompositeConfiguration
    try {
      val configuration = new PropertiesConfiguration(path)
      compositeConfiguration.addConfiguration(configuration)
    } catch {
      case e: ConfigurationException => {
        LOGGER.error("加载配置文件: " + path + ", 失败", e)
      }
    }
    LOGGER.info("加载配置文件: " + path + ", 成功。 ")
    compositeConfiguration
  }

  /**
    * @return java.util.Map<java.lang.String,java.util.HashMap<java.lang.String,java.lang.String>>
    * @author GuYongtao
    *         <p>获取table-字段 对应关系使用
    *         util.Map[String,util.HashMap[String, String结构保存</p>
    */
  def getKeysByType(): util.Map[String, util.HashMap[String, String]] = {
    val map = new util.HashMap[String, util.HashMap[String, String]]()
    // wechat, search
    val iteratorTable = tables.iterator()
    //对每个表进行遍历
    while (iteratorTable.hasNext) {
      //使用一个MAP保存一种对应关系
      val fieldMap = new util.HashMap[String, String]()
      //获取一个表
      val table: String = iteratorTable.next().toString
      //获取这个表的所有字段
      val fields = config.getKeys(table)
      //获取通用字段  这里暂时没有
      val commonKeys: util.Iterator[String] = config.getKeys("common").asInstanceOf[util.Iterator[String]]
      //将通用字段放到map结构中去
      while (commonKeys.hasNext) {
        val key = commonKeys.next()
        fieldMap.put(key.replace("common", table), config.getString(key))
      }
      //将每种表的私有字段放到map中去
      while (fields.hasNext) {
        val field = fields.next().toString
        fieldMap.put(field, config.getString(field))
      }
      map.put(table, fieldMap)
    }
    map
  }

  /**
    * @return java.util.Map<java.lang.String,java.lang.String>
    * @author GuYongtao
    *         <p>构建建表语句</p>
    */
  def getHiveTables(): util.Map[String, String] = {
    val hiveTableSqlMap: util.Map[String, String] = new util.HashMap[String, String]()

    //获取没中数据的建表语句
    tables.foreach(table => {

      var sql: String = s"CREATE external TABLE IF NOT EXISTS ${table} ("
      // tableFields = wechat.imei=string, wechat.imsi=string 多个
      val tableFields = config.getKeys(table.toString)
      tableFields.foreach(tableField => {
        //tableField = wechat.imsi=string 单个
        val fieldType = config.getProperty(tableField.toString.trim)
        // imsi
        val field = tableField.toString.split("\\.")(1)
        // CREATE external TABLE IF NOT EXISTS qq (imei
        sql = sql + field
        fieldType match {
          //就是将配置中的类型映射为HIVE 建表语句中的类型
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ => println("Nothing Matched!! " + fieldType)
        }
      })
      sql = sql.substring(0, sql.length - 1)
      sql = sql + s")STORED AS PARQUET location '${hive_root_path}${table}'"
      /*      sql = sql + s") partitioned by(year string,month string,day string) STORED AS PARQUET " +
              s"location '${path}${table}'"*/
      hiveTableSqlMap.put(table.toString, sql)
    })
    hiveTableSqlMap
  }


  /**
    * @return java.util.Map<java.lang.String,org.apache.spark.sql.types.StructType>
    * @author GuYongtao
    *         <p>使用 tableFieldsMap 对每种类型数据创建对应的Schema</p>
    */
  def createSchema(): util.Map[String, StructType] = {
    // schema  表结构
    /*   CREATE TABLE `warn_message` (
         //arrayStructType
         `id` int(11) NOT NULL AUTO_INCREMENT,
         `alarmRuleid` varchar(255) DEFAULT NULL,
         `alarmType` varchar(255) DEFAULT NULL,
         `sendType` varchar(255) DEFAULT NULL,
         `sendMobile` varchar(255) DEFAULT NULL,
         `sendEmail` varchar(255) DEFAULT NULL,
         `sendStatus` varchar(255) DEFAULT NULL,
         `senfInfo` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
         `hitTime` datetime DEFAULT NULL,
         `checkinTime` datetime DEFAULT NULL,
         `isRead` varchar(255) DEFAULT NULL,
         `readAccounts` varchar(255) DEFAULT NULL,
         `alarmaccounts` varchar(255) DEFAULT NULL,
         `accountid` varchar(11) DEFAULT NULL,
         PRIMARY KEY (`id`)
       ) ENGINE=MyISAM AUTO_INCREMENT=528 DEFAULT CHARSET=latin1;*/


    val mapStructType: util.Map[String, StructType] = new util.HashMap[String, StructType]()

    for (table <- tables) {
      //通过tableFieldsMap 拿到这个表的所有字段
      val tableFields = tableFieldsMap.get(table)
      //对这个字段进行遍历
      val keyIterator = tableFields.keySet().iterator()
      //创建ArrayBuffer
      var arrayStructType = ArrayBuffer[StructField]()
      while (keyIterator.hasNext) {
        val key = keyIterator.next()
        val value = tableFields.get(key)
        //将key拆分 获取 "."后面的部分作为数据字段
        val field = key.split("\\.")(1)
        value match {
          /* case "string" => arrayStructType += StructField(field, StringType, true)
           case "long"   => arrayStructType += StructField(field, LongType, true)
           case "double"   => arrayStructType += StructField(field, DoubleType, true)*/
          case "string" => arrayStructType += StructField(field, StringType, true)
          case "long" => arrayStructType += StructField(field, StringType, true)
          case "double" => arrayStructType += StructField(field, StringType, true)
          case _ => println("Nothing Matched!!" + value)
        }
      }
      val schema = StructType(arrayStructType)
      mapStructType.put(table.toString, schema)
    }
    mapStructType
  }

  /**
    * @return void
    * @author GuYongtao
    *         <p>创建HIVE表</p>
    */
  def createHiveTable(hiveContext: HiveContext): Unit = {
    //    HiveConfig.initParams()
    val keys = HiveConfig.hiveTableSQL.keySet()
    keys.foreach(key => {
      val sql = HiveConfig.hiveTableSQL.get(key)
      //通过hiveContext 和已经创建好的SQL语句去创建HIVE表
      try {
        hiveContext.sql(sql)
        println(s"创建表${key}成功")
      } catch {
        case e1: AlreadyExistsException => LOGGER.error(s"${key} 表已存在.", e1)
        case e2: Exception => LOGGER.error(s"${key} 表创建失败.", e2)
      }
    })
  }



}
