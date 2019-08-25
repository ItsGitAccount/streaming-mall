package com.atguigu.streamingmall.realtime.util

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {


  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }


  // 批量添加
  def indexBulk(indexName: String, etype: String = "_doc", sourceList: List[(String, Any)])= {
    if (sourceList.size > 0) {
      val jestClient = getClient
      val builder = new Bulk.Builder

      for ((id,data) <- sourceList) {
        val index: Index = new Index.Builder(data).index(indexName).`type`(etype).id(id).build()
        builder.addAction(index)
      }

      val result: BulkResult = jestClient.execute(builder.build())
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println("stored : " + items.size())

      close(jestClient)
    }
  }

  // 创建es 表的元素
  // index
  // type
  // id
  // source
//  def main(args: Array[String]): Unit = {
//
//    /**
//      * 单条插入测试
//      * val jestClient = getClient
//      * val action = new Index.Builder(Hadoop("zhangjunning",30)).index("hadoop").`type`("doc").id("1").build()
//      *jestClient.execute(action)
//      * close(jestClient)
//      */
//  }




}

//case class Hadoop(name: String, age: Int)
