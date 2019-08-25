package com.atguigu.streamingmall.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.streamingmall.publisher.bean.Option;
import com.atguigu.streamingmall.publisher.bean.Stat;
import com.atguigu.streamingmall.publisher.service.AmountService;
import com.atguigu.streamingmall.publisher.service.PublisherService;
import com.atguigu.streamingmall.publisher.service.SaleDetailService;
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
	@Autowired
	AmountService amountService;
	@Autowired
	SaleDetailService saleDetailService;

	@GetMapping("realtime-total")
	public String getTotalContByDay(@RequestParam("date") String date){

		Long totalCount = publisherService.countAllActiveOfDay(date);
		Double sumAllAmountOfDay = amountService.sumAllAmountOfDay(date);

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

		Map map2 = new HashMap();
		map2.put("id","order_amount");
		map2.put("name","新增交易额");
		map2.put("value",sumAllAmountOfDay);
		listMap.add(map2);

		String jsonString = JSON.toJSONString(listMap);
		return jsonString;

	}



	@RequestMapping("realtime-hours")
	public String getPerHourNewActive(@RequestParam("id") String id,@RequestParam("date") String date) {

		if ("dau".equals(id)) { //日活
			Map<String, Map> allHourMap = new HashMap<>();

			String yesterday = getYesterday(date);
			Map<String, Long> yesterdayMap = publisherService.countPerHourNewActive(yesterday);
			Map<String, Long> todayMap = publisherService.countPerHourNewActive(date);

			allHourMap.put("yesterday", yesterdayMap);
			allHourMap.put("today", todayMap);

			String jsonString = JSON.toJSONString(allHourMap);

			return jsonString;
		}else if ("order_amount".equals(id)){ // 交易额
			Map<String, Map> amountMap = new HashMap<>();

			String yesterday = getYesterday(date);
			Map<String, Double> yesterdayMap = amountService.sumPerHourNewAmount(yesterday);
			Map<String, Double> todayMap = amountService.sumPerHourNewAmount(date);

			amountMap.put("yesterday", yesterdayMap);
			amountMap.put("today", todayMap);

			String jsonString = JSON.toJSONString(amountMap);

			return jsonString;
		}
		return null;
	}


	@RequestMapping("sale_detail")
	public String saleDetailController(@RequestParam("date") String date,@RequestParam("startpage") int startpage,@RequestParam("size") int size,@RequestParam("keyword") String keyword){
		Map saleDetailMap = saleDetailService.getSaleDetail(date, startpage, size, keyword);

		Long total = (Long) saleDetailMap.get("total");
		List detail = (List) saleDetailMap.get("detail");
		Map genderMap = (Map) saleDetailMap.get("genderAgg");
		Map ageMap = (Map) saleDetailMap.get("ageAgg");

		// 性别饼图
		Long maleCount = (Long) genderMap.get("M");
		Long femaleCount = (Long) genderMap.get("F");

		Double maleRatio = Math.round(maleCount*1000D/total) / 10D;
		Double femaleRatio = Math.round(femaleCount*1000D/total) / 10D;

		List<Option> genderOptionList = new ArrayList<>();
		genderOptionList.add(new Option("男",maleRatio));
		genderOptionList.add(new Option("女",femaleRatio));

		Stat genderStat = new Stat("性别占比",genderOptionList);


		// 年龄饼图

		Long countUnder20 = 0L;
		Long countFrom20until30 = 0L;
		Long countbeyond30 = 0L;

		for (Object o : ageMap.entrySet()) {
			Map.Entry entry = (Map.Entry) o;
			String age = (String) entry.getKey();
			Long count = (Long) entry.getValue();

			if(Integer.parseInt(age) < 20){
				countUnder20 += count;
			}else if(Integer.parseInt(age) < 30 && Integer.parseInt(age) >= 20){
				countFrom20until30 += count;
			}else {
				countbeyond30 += count;
			}
		}

		double countUnder20Ratio = Math.round(countUnder20 * 1000D / total) / 10D;
		double countFrom20until30Ratio = Math.round(countFrom20until30 * 1000D / total) / 10D;
		double countbeyond30Ratio = Math.round(countbeyond30 * 1000D / total) / 10D;

		Option countUnder20Option = new Option("20岁以下", countUnder20Ratio);
		Option countFrom20until30Option = new Option("20岁到30岁", countFrom20until30Ratio);
		Option countbeyond30Option = new Option("30岁以上", countbeyond30Ratio);

		List<Option> ageOptionList = new ArrayList<>();
		ageOptionList.add(countUnder20Option);
		ageOptionList.add(countFrom20until30Option);
		ageOptionList.add(countbeyond30Option);

		Stat ageStat = new Stat("年龄占比", ageOptionList);

		List<Stat> statList = new ArrayList<>();
		statList.add(genderStat);
		statList.add(ageStat);

		Map<String,Object> viewMap = new HashMap<>();
		viewMap.put("total",total);
		viewMap.put("stat",statList);
		viewMap.put("detail",detail);

		String resultJson = JSON.toJSONString(viewMap);

		return  resultJson;
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
