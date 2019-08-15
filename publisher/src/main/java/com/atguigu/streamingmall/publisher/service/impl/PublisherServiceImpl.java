package com.atguigu.streamingmall.publisher.service.impl;

import com.atguigu.streamingmall.publisher.mapper.PublisherMapper;
import com.atguigu.streamingmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

	@Autowired
	PublisherMapper dataMapper;

	@Override
	public Long countAllActiveOfDay(String date) {
		return dataMapper.getTotalCount(date);
	}

	@Override
	public Map<String, Long> countPerHourNewActive(String date) {
		List<Map> perHourCount = dataMapper.getPerHourCount(date);
		HashMap<String, Long> longStringHashMap = new HashMap<>();
		for (Map map : perHourCount) {
			longStringHashMap.put((String)map.get("LOGHOUR"),(Long) map.get("CT"));
		}
		return longStringHashMap;
	}
}
