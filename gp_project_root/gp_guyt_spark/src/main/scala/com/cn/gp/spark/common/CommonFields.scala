package com.cn.gp.spark.common

object CommonFields {
  // 运行参数名称
  final val BROKER_LIST = "broker_list"
  final val GROUP_ID = "group_id"
  final val TOPICS = "topics"


  // HIVE
  final val HIVE_SITE_XML = "spark/hive/hive-site.xml"
  final val HIVE_META_STORE_DIR = "/user/hive/warehouse/external/"
  final val FIELD_MAPPING = "es/mapping/field-mapping.properties"
  final val HADOOP_HTTP_URL = "hdfs://gp-guyt-1:8020"

  // 字段名称
  final val TABLE_NAME = "table_name"
  final val COLLECT_TIME = "collect_time"
  final val INDEX_DATE_NAME = "index_date"

  // HBase
  final val NAME_SPACE = "gp"
  final val COLUMN_FAMILY = "cf"
  final val CACHE_PHONE = "cache:phone"

}
