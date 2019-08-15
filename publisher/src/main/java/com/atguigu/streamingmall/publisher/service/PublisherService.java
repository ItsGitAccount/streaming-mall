package com.atguigu.streamingmall.publisher.service;

import java.util.Map;

public interface PublisherService {

	public Long countAllActiveOfDay(String date);

	public Map<String, Long> countPerHourNewActive(String date);
}
