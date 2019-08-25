package com.atguigu.streamingmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.streamingmall.common.constant.MallConstant
import com.atguigu.streamingmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.streamingmall.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization


import scala.collection.mutable.ListBuffer

object SaleAPP {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("SaleAPP").setMaster("local[*]"))
    val ssc = new StreamingContext(sc,Seconds(5))



    // 读取数据
    val orderInfoDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MallConstant.KAFKA_TOPIC_ORDER,ssc)
    val orderDetailDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MallConstant.KAFKA_TOPIC_ORDER_DETAIL,ssc)
    val userInfoDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MallConstant.KAFKA_TOPIC_USER,ssc)

    // 利用样例类转换结构
    val orderDstream: DStream[OrderInfo] = orderInfoDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      // 电话号脱敏处理
      orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4)._1 + "*******"
      // 添加日期字段
      val split: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = split(0)
      orderInfo.create_hour = split(1).split(":")(0)

      orderInfo
    }
    // orderDetail
    val detailDstream: DStream[OrderDetail] = orderDetailDstream.map { record =>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }

    // userInfo
    val userDstream: DStream[UserInfo] = userInfoDstream.map { record =>
      val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
      userInfo
    }


    //双流join ： 1、fullOutJoin 另一个流  2、fullOutJoin 缓存中的另一个流  3、将自己存入缓存

    // join需要k-v结构
    val orderInfoWithKey: DStream[(String, OrderInfo)] = orderDstream.map(info => (info.id,info))
    val orderDetailWithKey: DStream[(String, OrderDetail)] = detailDstream.map(detail => (detail.order_id,detail))


    // fullOutJoin 另一个流
    val firstJoinDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKey.fullOuterJoin(orderDetailWithKey)

    // 主表!=None且从表也 ！=None
    // 1. 合并为宽表对象
    // 2. 写缓存
    // 3. join 缓存
    val mergedDstream: DStream[SaleDetail] = firstJoinDstream.flatMap { case (id, (orderInfoOpt, orderDetailOpt)) =>
      implicit val json4s = org.json4s.DefaultFormats

      // 用户暂存saleDetail
      val listBuffer: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
      val jedis = new Jedis("hadoop102", 6379)

      // 1.while orderInfo is not none && orderDetail is not none either,merge them
      if (orderInfoOpt != None) {
        val orderInfo: OrderInfo = orderInfoOpt.get
        val orderInfoKey: String = "order_info" + orderInfo.id
        if (orderDetailOpt != None) {
         val orderDetail: OrderDetail = orderDetailOpt.get
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          listBuffer += saleDetail
        }

        // 2.cache orderInfo into redis
        // string✔ set zset List hash
        val orderInfoJsonString: String = Serialization.write(orderInfo)


        jedis.setex(orderInfoKey, 3600, orderInfoJsonString)

        // 3.query from redis if there exist orderDetail that has the same id with orderInfo,true ? merge them
        // orderDetail : string set✔ zset List hash
        val orderDetailKey: String = "order_detail" + orderInfo.id
        val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
        import collection.JavaConversions._
        for (detail <- orderDetailSet) {
          val orderDetailFromCache: OrderDetail = JSON.parseObject(detail, classOf[OrderDetail])
          val saleDetailFromOrderDetailCache = new SaleDetail(orderInfo, orderDetailFromCache)
          listBuffer += saleDetailFromOrderDetailCache
        }


      } else {
        if (orderDetailOpt != None) { // orderInfo does not exist

          val orderDetail: OrderDetail = orderDetailOpt.get

          // 1.chche orderDetail self
          val orderDetailJsonString: String = Serialization.write(orderDetail)
          val orderDetailKey: String = "order_detail" + orderDetail.order_id
          jedis.sadd(orderDetailKey, orderDetailJsonString)
          jedis.expire(orderDetailKey, 3600)

          // 2.query from cache if exist orderInfo,true? merge them
          val orderInfoKey: String = "order_info" + orderDetail.order_id
          val orderInfoFromCache: String = jedis.get(orderInfoKey)
          if (orderInfoFromCache != null && orderDetailJsonString.size > 0) {
            val orderInfoToCase: OrderInfo = JSON.parseObject(orderInfoFromCache, classOf[OrderInfo])
            val saleDetailFromOrderInfoCache = new SaleDetail(orderInfoToCase, orderDetail)
            listBuffer += saleDetailFromOrderInfoCache
          }

          jedis.close()

        }
      }

      listBuffer

    }

//   mergedDstream.foreachRDD{rdd=>
//     println(rdd.collect().mkString("\n"))
//   }

    // save userInfo to cache
    // string set zset List hash✔
    userDstream.foreachRDD{rdd=>
      val userList: List[UserInfo] = rdd.collect().toList
      val jedis1 = new Jedis("hadoop102",6379)
      implicit val formats=org.json4s.DefaultFormats
      for (user <- userList) {
        val userInfoJsonString: String = Serialization.write(user)
        val userInfoKey: String = "user_info"
        jedis1.hset(userInfoKey, user.id, userInfoJsonString)
      }
      jedis1.close()
    }


    // 双流join的结果和userInfo再join
    val finalJoinDstream: DStream[SaleDetail] = mergedDstream.mapPartitions { iter =>
      val listBuffer1 = new ListBuffer[SaleDetail]()
      val jedis2 = new Jedis("hadoop102", 6379)
      for (elem <- iter) {
        val users: String = jedis2.hget("user_info", elem.user_id)
        if (users != null){
          val userInfoFromCache: UserInfo = JSON.parseObject(users, classOf[UserInfo])
          elem.mergeUserInfo(userInfoFromCache)
        }
        listBuffer1 += elem
      }
      jedis2.close()
      listBuffer1.toIterator
    }

//    finalJoinDstream.foreachRDD{rdd=>
//      println(rdd.collect().mkString("\n"))
//    }


//    // 将宽表存入es
    finalJoinDstream.foreachRDD{rdd=>
      rdd.foreachPartition{saleDetailIter =>
        val dataList: List[(String, SaleDetail)] = saleDetailIter.map(saleDetail => (saleDetail.order_detail_id,saleDetail) ).toList
        MyEsUtil.indexBulk(MallConstant.ES_INDEX_SALE_DETAIL,sourceList = dataList)
      }
    }




    ssc.start()
    ssc.awaitTermination()

  }

}
