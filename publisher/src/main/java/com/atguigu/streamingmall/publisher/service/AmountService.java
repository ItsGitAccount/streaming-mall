package com.atguigu.streamingmall.publisher.service;

import java.util.Map;

public interface AmountService {

	public Double sumAllAmountOfDay(String date);

	public Map<String, Double> sumPerHourNewAmount(String date);
}
