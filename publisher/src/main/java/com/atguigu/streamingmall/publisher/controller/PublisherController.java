package com.atguigu.streamingmall.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.streamingmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

	@Autowired
	PublisherService publisherService;

	@GetMapping("realtime-total")
	public String getTotalContByDay(@RequestParam("date") String date){

		Long totalCount = publisherService.countAllActiveOfDay(date);

		List<Map> listMap = new ArrayList<>();
		Map map = new HashMap();
		map.put("id","dau");
		map.put("name","新增日活");
		map.put("value",totalCount);
		listMap.add(map);

		Map map1 = new HashMap();
		map1.put("id","new_mid");
		map1.put("name","新增设备");
		map1.put("value",314);
		listMap.add(map1);

		String jsonString = JSON.toJSONString(listMap);
		return jsonString;

	}



	@RequestMapping("realtime-hours")
	public String getPerHourNewActive(@RequestParam("id") String id,@RequestParam("date") String date) {

		if ("dau".equals(id)) {
			Map<String, Map> allHourMap = new HashMap<>();

			String yesterday = getYesterday(date);
			Map<String, Long> yesterdayMap = publisherService.countPerHourNewActive(yesterday);
			Map<String, Long> todayMap = publisherService.countPerHourNewActive(date);

			allHourMap.put("yesterday", yesterdayMap);
			allHourMap.put("today", todayMap);

			String jsonString = JSON.toJSONString(allHourMap);

			return jsonString;
		}
		return null;
	}


	private String getYesterday(String today){
		String yesterday = null;
		try {
			Date parseDate = new SimpleDateFormat("yyyy-MM-dd").parse(today);
			yesterday = DateUtils.addDays(parseDate, -1).toString();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return yesterday;
	}

}
