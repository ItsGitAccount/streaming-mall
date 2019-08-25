package com.atguigu.streamingmall.publisher.mapper;

import java.util.List;
import java.util.Map;


public interface AmountMapper {

	Double getTotalSum(String date);

	List<Map> getPerHourSum(String date);


}
