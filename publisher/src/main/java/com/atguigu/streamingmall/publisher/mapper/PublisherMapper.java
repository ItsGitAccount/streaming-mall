package com.atguigu.streamingmall.publisher.mapper;

import java.util.List;
import java.util.Map;


public interface PublisherMapper {

	Long getTotalCount(String date);

	List<Map> getPerHourCount(String date);


}
