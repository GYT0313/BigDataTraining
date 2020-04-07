package com.cn.gp.spark.streaming.kafka.kafka2hbase

/**
  * @author GuYongtao 
  * @version 1.0.0
  *          <p></p>
  */
object Test {
  def main(args: Array[String]): Unit = {

    // 删表
//    DataRelationStreaming.deleteHbaseTable(DataRelationStreaming.relationFields)
    ComplexDataRelationStreaming.deleteHbaseTable(ComplexDataRelationStreaming.complexRelationField);

    Thread.sleep(5000);

    // 建表
//    DataRelationStreaming.initRelationHbaseTable(DataRelationStreaming.relationFields)
    ComplexDataRelationStreaming.initRelationHbaseTable(ComplexDataRelationStreaming.complexRelationField)



  /*  val table = HBaseTableUtil.getTable("test:relation")
    val exists = HBaseSearchServiceImpl.existsRowkey(table, "1c-41-cd-ae-4f-4f")
    println(exists)*/
    //從索引表中获取总关联表的rowkey

   /* val baseSearchService : HBaseSearchService = new HBaseSearchServiceImpl()
    val get = new Get("1c-41-cd-ae-4f-4f".getBytes())
    get.setMaxVersions(100)
     val set = new util.HashSet[String]()
    val extrator = new SingleColumnMultiVersionRowExtrator("cf".getBytes(),"imei".getBytes(),set)
    val clSet = baseSearchService.search("test:relation",get,extrator)
    println(clSet)*/

    //println(("aaaaaaaaaaaaaa").hashCode() & Integer.MAX_VALUE)


/*    val list  = new util.ArrayList[String]

    DataRelationStreaming.relationFields.foreach(x=>{
      //把这个list当成hbase
      list.add(x)
    })*/

  }
}
