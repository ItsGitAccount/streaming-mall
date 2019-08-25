package com.atguigu.streamingmall.publisher.service.impl;

import com.atguigu.streamingmall.publisher.mapper.AmountMapper;
import com.atguigu.streamingmall.publisher.service.AmountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class AmountServiceImpl implements AmountService {

	@Autowired
	AmountMapper amountMapper;


	@Override
	public Double sumAllAmountOfDay(String date) {
		return amountMapper.getTotalSum(date);
	}

	@Override
	public Map<String, Double> sumPerHourNewAmount(String date) {
		List<Map> perHourSum = amountMapper.getPerHourSum(date);
		Map resultMap = new HashMap();

		for (Map map : perHourSum) {

			resultMap.put(map.get("CREATE_HOUR"), map.get("SUM"));

		}
		return resultMap;

	}
}
