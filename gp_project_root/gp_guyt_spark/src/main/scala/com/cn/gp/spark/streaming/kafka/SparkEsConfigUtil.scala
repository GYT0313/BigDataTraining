package com.cn.gp.spark.streaming.kafka

object SparkEsConfigUtil {
  val ES_NODES = "es.nodes"
  val ES_PORT = "es.port"
  val ES_CLUSTER_NAME = "es.clustername"

  def getEsParam(idField: String): Map[String, String] = {
    // es.mapping.id  - ES中的主键
    Map[String, String](
      "es.mapping.id" -> idField,
      ES_NODES -> "gp-guyt-2",
      ES_PORT -> "9200",
      ES_CLUSTER_NAME -> "gp-application",
      "es.batch.size.entries" -> "6000",
      "es.batch.size.bytes" -> "300000000",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true"
    )
  }
}
