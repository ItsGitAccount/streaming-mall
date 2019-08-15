package com.atguigu.gtreamingmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.streamingmall.common.constant.MallConstant
import com.atguigu.streamingmall.realtime.util.{MyKafkaUtil, Startuplog}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {


  // TODO 消费kafka，进行数据清洗，去重

  def main(args: Array[String]): Unit = {
    //获取上下文环境
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))


    val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MallConstant.KAFKA_TOPIC_STARTUP,ssc)

//    dStream.foreachRDD{rdd=>
//      println(rdd.map(_.value()).collect().mkString("\n"))
//
//    }

      val startupLogDstream: DStream[Startuplog] = dStream.map { record =>
      val startupJsonString: String = record.value()
      val startuplog: Startuplog = JSON.parseObject(startupJsonString, classOf[Startuplog])

//      println(startuplog.ts)

      val dateString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startuplog.ts))
      val split = dateString.split(" ")
      startuplog.logDate = split(0)
      startuplog.logHour = split(1)

      startuplog

    }

    // 将redis中的数据取出，对新数据进行过滤
    startupLogDstream.cache()

    val filteredDstream: DStream[Startuplog] = startupLogDstream.transform { rdd =>

      println("过滤前：" + rdd.count())

      val jedis = new Jedis("hadoop102", 6379)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedisMembers: util.Set[String] = jedis.smembers("dau:" + date)
      val brdMemb: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(jedisMembers)

      jedis.close()


      val filterRdd: RDD[Startuplog] = rdd.filter { record =>
        !brdMemb.value.contains(record.mid)
      }

      println("过滤后：" + filterRdd.count())
      filterRdd


    }

    filteredDstream.cache()
    // 批次内去重
    val kvDstream: DStream[(String, Startuplog)] = filteredDstream.map { log =>
      (log.mid, log)
    }

    val groupDstream: DStream[(String, Iterable[Startuplog])] = kvDstream.groupByKey()

    val innerFilterDstream: DStream[Startuplog] = groupDstream.flatMap {
      case (mid, iter) =>
        iter.take(1)
    }


    innerFilterDstream.cache()

    // 存入redis的set中
    innerFilterDstream.foreachRDD{rdd=>
      rdd.foreachPartition{iter=>
        val jedis = new Jedis("hadoop102",6379)

        for(record <- iter){

          val key: String = "dau:"+record.logDate

          jedis.sadd(key,record.mid)
        }
        jedis.close()
      }
    }


    // 将去重后的数据存入hbase

    import org.apache.phoenix.spark._

    innerFilterDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix(
        "STREAMINGMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    }


















    println("消费启动")
    //循环启动
    ssc.start()
    ssc.awaitTermination()
  }



}
