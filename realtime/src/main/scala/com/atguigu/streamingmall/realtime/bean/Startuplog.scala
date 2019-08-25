package com.atguigu.streamingmall.realtime.bean

case class Startuplog(
                         mid:String,
                         uid:String,
                         appid:String,
                         area:String,
                         os:String,
                         ch:String,
                         `type`:String,
                         vs:String,
                         var logDate:String,
                         var logHour:String,
                         ts:Long
                         )
