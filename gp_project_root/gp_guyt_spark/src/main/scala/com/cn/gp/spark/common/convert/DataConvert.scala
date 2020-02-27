package com.cn.gp.spark.common.convert


import com.cn.gp.common.config.ConfigUtil
import org.apache.spark.internal.Logging
import scala.collection.JavaConversions._

/**
  * @author GuYongtao
  * @version 1.0.0
  *          <p>数据类型转换</p>
  */
object DataConvert extends Serializable {
  val fieldMappingPath = "es/mapping/field-mapping.properties"

  private val typeFieldMap: java.util.HashMap[String, java.util.HashMap[String, String]] = getEsFieldTypeMap()

  /**
    * @return java.util.Map<java.lang.String,java.lang.Object>
    * @author GuYongtao
    *         <p>转换为ES ObjectMap</p>
    */
  def strMap2EsObjectMap(map: java.util.Map[String, String]): java.util.Map[String, Object] = {
    val objectMap: java.util.HashMap[String, Object] = new java.util.HashMap[String, Object]()

    // ['wechat', [字段1: long, 字段2: int]]
    val dataType = map.get("table")
    // [字段1: long, 字段2: int]
    val fieldMap = typeFieldMap.get(dataType)

    // 配置文件中的字段
//    val configKeys = fieldMap.keySet()
    // 数据里的字段
    val dataKeys = map.keySet().iterator()

    while (dataKeys.hasNext) {
      val key = dataKeys.next()
      var dataType: String = "string"
      // 如果配置文件中包含数据中的key
      if (fieldMap.containsKey(key)) {
        dataType = fieldMap.get(key).toString
      }

      // 模式匹配
      dataType match {
        case "long" => BaseDataConvert.mapString2Long(map, key, objectMap)
        case "string" => BaseDataConvert.mapString2String(map, key, objectMap)
        case "double" => BaseDataConvert.mapString2Double(map, key, objectMap)
        case "_" => BaseDataConvert.mapString2String(map, key, objectMap)
      }
    }
    objectMap
  }

  /**
    * @return java.util.HashMap<java.lang.String,java.util.HashMap<java.lang.String,java.lang.String>>
    * @author GuYongtao
    *         <p>获取fieldMappingPath配置文件内容，里面包含各字段需要转换的类型</p>
    */
  def getEsFieldTypeMap(): java.util.HashMap[String, java.util.HashMap[String, String]] = {
    // ["wechat": ["phone_mac": "string", "latitude": "long"]]
    val mapTypes = new java.util.HashMap[String, java.util.HashMap[String, String]]
    val properties = ConfigUtil.getInstance().getProperties(fieldMappingPath)
    val tables = properties.get("tables").toString.split(",")
    val tableFields = properties.keySet()

    // 按table封装各字段的数据类型
    tables.foreach(table => {
      val mapType = new java.util.HashMap[String, String]()
      tableFields.foreach(tableField => {
        if (tableField.toString.startsWith(table)) {
          // wechat.imei=string
          val key = tableField.toString.split("\\.")(1)
          val value = properties.get(tableField).toString
          mapType.put(key, value)
        }
      })
      mapTypes.put(table, mapType)
    })
    mapTypes
  }


  def main(args: Array[String]): Unit = {
    val a = getEsFieldTypeMap()
  }

}
