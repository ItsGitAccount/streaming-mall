package com.atguigu.streamingmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.streamingmall.common.constant.MallConstant
import com.atguigu.streamingmall.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.streamingmall.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._


/**
  * 需求：
  * 同一设备
  * 五分钟内
  * 三次及以上,不同账号登录
  * 并领取优惠卷
  * 没有浏览商品
  * 同一设备一分钟只记录一次预警
  */

object AlertAPP {

  def main(args: Array[String]): Unit = {

    val spark = new SparkContext(new SparkConf().setAppName("AlertAPP").setMaster("local[*]"))
    val ssc = new StreamingContext(spark, Seconds(5))

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MallConstant.KAFKA_TOPIC_EVENT, ssc)


    // 转换样例类
    val eventDstream: DStream[EventInfo] = recordDstream.map { record =>
      val eventInfo: EventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
      eventInfo
    }

    eventDstream.cache()

    // 五分钟内
    val perFiveDstrem: DStream[EventInfo] = eventDstream.window(Seconds(300), Seconds(5))


    // 按设备分组
    val groupDstream: DStream[(String, Iterable[EventInfo])] = perFiveDstrem.map { event =>
      (event.mid, event)
    }.groupByKey()


    // 三次及以上不同账户登录并领优惠券，没有浏览商品
    val alertInfoDstream: DStream[(Boolean, AlertInfo)] = groupDstream.map { case (mid, iter) =>
      val userSet = new util.HashSet[String]()
      val itemSet = new util.HashSet[String]()
      val eventList = new util.ArrayList[String]()

      var isClickItem = false // false 未浏览， true 浏览

      // 满足条件：领取优惠卷时的 uid >= 3 && 没有 clickItem
      breakable {
        for (elem <- iter) {
          eventList.add(elem.evid)
          // 如果浏览商品就中断判断
          if (elem.evid == "clickItem") {
            isClickItem = true
            break()
          }

          if (elem.evid == "coupon") {
            userSet.add(elem.uid)
            itemSet.add(elem.itemid)
          }

        }
      }

      // （是否满足条件，预警信息）
      (userSet.size() >= 3 && !isClickItem, AlertInfo(mid, userSet, itemSet, eventList, System.currentTimeMillis()))

    }

    alertInfoDstream.cache()

    //    alertInfoDstream.foreachRDD{rdd=>
    //      println(rdd.collect().mkString("\n"))
    //    }


    // 取出满足条件（有恶意行为）的事件，并去除标签
    val filterdDstream: DStream[AlertInfo] = alertInfoDstream.filter(_._1).map(_._2)


    // 将数据保存至 elasticsearch中
    filterdDstream.foreachRDD { rdd =>
      rdd.foreachPartition { info =>

        val list: List[AlertInfo] = info.toList

        // 提取主键 ：mid+min 达到 每分钟去重的效果
        val alertInfoList: List[(String, AlertInfo)] = list.map { alertInfoItm =>
          (alertInfoItm.mid + alertInfoItm.ts / 1000 / 60, alertInfoItm)
        }

        // 批量保存到es
        MyEsUtil.indexBulk(MallConstant.COUPON_ALERT_INFO,sourceList = alertInfoList)
      }
    }


    ssc.start()
    ssc.awaitTermination()

  }


}
