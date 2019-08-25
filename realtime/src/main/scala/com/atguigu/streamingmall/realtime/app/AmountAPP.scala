package com.atguigu.streamingmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.streamingmall.common.constant.MallConstant
import com.atguigu.streamingmall.realtime.bean.OrderInfo
import com.atguigu.streamingmall.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AmountAPP {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("AmountAPP").setMaster("local[*]")
    val ssc = new StreamingContext(new SparkContext(conf),Seconds(5))


    // 从kafka消费数据
    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MallConstant.KAFKA_TOPIC_ORDER,ssc)

    val handledDstream: DStream[OrderInfo] = recordDstream.map { record =>
      // 封装进样例类
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      // 对敏感数据进行数据脱敏，consignee_tel，delivery_address
      val time: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = time(0) //加日期和小时字段，便于后期符合接口要求
      orderInfo.create_hour = time(1).split(":")(0)
      orderInfo.consignee_tel = "*******" + orderInfo.consignee_tel.splitAt(7)._2 //电话号处理成 *******9683

      orderInfo

    }

    import org.apache.phoenix.spark._

    handledDstream.foreachRDD{rdd=>

      println(rdd.collect().mkString("\n"))

      // 将数据存进(phoenix) hbase
      rdd.saveToPhoenix("STREAMINGMALL_ORDER_INFO",
        Seq(
          "ID","PROVINCE_ID",
          "CONSIGNEE",
          "ORDER_COMMENT",
          "CONSIGNEE_TEL",
          "ORDER_STATUS",
          "PAYMENT_WAY",
          "USER_ID",
          "IMG_URL",
          "TOTAL_AMOUNT",
          "EXPIRE_TIME",
          "DELIVERY_ADDRESS",
          "CREATE_TIME",
          "OPERATE_TIME",
          "TRACKING_NO",
          "PARENT_ORDER_ID",
          "OUT_TRADE_NO",
          "TRADE_BODY",
          "CREATE_DATE",
          "CREATE_HOUR"
        ),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181"))

        }

    ssc.start()
    ssc.awaitTermination()

  }

}
