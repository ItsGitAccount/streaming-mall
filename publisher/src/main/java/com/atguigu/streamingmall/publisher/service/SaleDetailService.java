package com.atguigu.streamingmall.publisher.service;

import java.util.Map;

public interface SaleDetailService {

	Map getSaleDetail(String date,int startPage,int size,String keyWord);
}
