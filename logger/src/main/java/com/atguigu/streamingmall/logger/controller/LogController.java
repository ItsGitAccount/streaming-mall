package com.atguigu.streamingmall.logger.controller;


//请求格式：http://logserver/log?logString={ ... }


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.streamingmall.common.constant.MallConstant;
import lombok.extern.log4j.Log4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController // 等价于 ：@Controller + @RequestBody
@Log4j
public class LogController {

	@Autowired
	KafkaTemplate<String,String> kafkaTemplate;

	//@RequestMapping(params = "log",method = POST) 老方式
	@PostMapping("log")
	public String doLog(@RequestParam("logString") String logString){

		//1.为日志加上服务器端的时间戳
		JSONObject jsonObject = JSON.parseObject(logString);
		jsonObject.put("ts", System.currentTimeMillis());

		//2.将数据写入log文件

		String jsonString = jsonObject.toJSONString();
		log.info(jsonString);

		//3.将数据发往kafka


		if("startup".equals(jsonObject.getString("type"))) {
			kafkaTemplate.send(MallConstant.KAFKA_TOPIC_STARTUP, jsonString);
		}else {
			kafkaTemplate.send(MallConstant.KAFKA_TOPIC_EVENT, jsonString);

		}

		return "SUCCESS";
	}

}
