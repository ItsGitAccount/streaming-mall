package com.atguigu.streamingmall.publisher.service.impl;

import com.atguigu.streamingmall.publisher.service.SaleDetailService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SaleDetailServiceImpl implements SaleDetailService{

	@Autowired
	JestClient jestClient;

	@Override
	public Map getSaleDetail(String date, int startPage, int size, String keyWord) {

		String query = "GET gmall_sale_detail/_search\n" +
				"{\n" +
				"  \"query\": {\n" +
				"    \"bool\": {\n" +
				"      \"filter\": {\n" +
				"        \"term\": {\n" +
				"          \"dt\": \"2019-08-25\"\n" +
				"        }\n" +
				"      }, \n" +
				"      \"must\": [\n" +
				"        {\"match\":{\n" +
				"          \"sku_name\": {\n" +
				"            \"query\": \"小米手机\",\n" +
				"            \"operator\": \"and\"\n" +
				"          }\n" +
				"         } \n" +
				"          \n" +
				"        }\n" +
				"     ] \n" +
				"    }\n" +
				"  }\n" +
				"  , \"aggs\":  {\n" +
				"    \"groupby_age\": {\n" +
				"      \"terms\": {\n" +
				"        \"field\": \"user_age\" ,\n" +
				"        \"size\": 100\n" +
				"      }\n" +
				"    },\n" +
				"    \"groupby_gender\":{\n" +
				"      \"terms\": {\n" +
				"        \"field\": \"user_gender\",\n" +
				"        \"size\": 2\n" +
				"      }\n" +
				"    }\n" +
				"  }\n" +
				"  ,\n" +
				"  \"size\": 2\n" +
				"  , \"from\": 0\n" +
				"}";


//		Search search = new Search.Builder(query).build();
//		try {
//			jestClient.execute(search);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}


		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		//过滤、匹配
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
		boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyWord).operator(MatchQueryBuilder.Operator.AND));
		searchSourceBuilder.query(boolQueryBuilder);
		// 聚合
		TermsBuilder genderAggs = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
		TermsBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(100);
		searchSourceBuilder.aggregation(genderAggs);
		searchSourceBuilder.aggregation(ageAggs);
		// 分页
		searchSourceBuilder.from( (startPage - 1)*size );
		searchSourceBuilder.size(size);

		Search search = new Search.Builder(searchSourceBuilder.toString()).build();
		Map<String, Object> map = new HashMap<>();
		try {
			SearchResult searchResult = jestClient.execute(search);

			//总数
			map.put("total",searchResult.getTotal());
			//明细
			List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
			List<Map> saleList = new ArrayList<>();
			for (SearchResult.Hit<Map, Void> hit : hits) {
				saleList.add(hit.source);
			}
			map.put("detail",saleList);
			//聚合结果
			MetricAggregation aggregations = searchResult.getAggregations();
			// 性别
			List<TermsAggregation.Entry> bukets = aggregations.getTermsAggregation("groupby_gender").getBuckets();
			Map genderMap = new HashMap();
			for (TermsAggregation.Entry buket : bukets) {
				genderMap.put(buket.getKey(),buket.getCount());
			}
			map.put("genderAgg",genderMap);
			// 年龄
			List<TermsAggregation.Entry> bukets1 = aggregations.getTermsAggregation("groupby_age").getBuckets();
			Map ageMap = new HashMap();
			for (TermsAggregation.Entry buket : bukets1) {
				ageMap.put(buket.getKey(),buket.getCount());
			}
			map.put("ageAgg",ageMap);

		} catch (IOException e) {
			e.printStackTrace();
		}

		return map;
	}
}
