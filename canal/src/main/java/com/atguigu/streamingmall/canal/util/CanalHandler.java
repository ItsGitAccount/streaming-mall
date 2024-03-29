package com.atguigu.streamingmall.canal.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.streamingmall.common.constant.MallConstant;

import java.util.List;
import java.util.Random;

public class CanalHandler {


	public static void handle(String tableNaem, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {


		// 订单表
		if ("order_info".equals(tableNaem) && CanalEntry.EventType.INSERT.equals(eventType)) {

			rowDataToKafka(rowDataList,MallConstant.KAFKA_TOPIC_ORDER);
		// 订单详情表
		}else if ("order_detail".equals(tableNaem) && CanalEntry.EventType.INSERT.equals(eventType)){

			rowDataToKafka(rowDataList,MallConstant.KAFKA_TOPIC_ORDER_DETAIL);
		// 用户信息详情
		}else if ("user_info".equals(tableNaem) && eventType.equals(CanalEntry.EventType.INSERT) || eventType.equals(CanalEntry.EventType.UPDATE )){

			rowDataToKafka(rowDataList,MallConstant.KAFKA_TOPIC_USER);
		}

	}



	private static void rowDataToKafka(List<CanalEntry.RowData> rowDataList,String topic) {

		for (CanalEntry.RowData rowData : rowDataList) {

			List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
			JSONObject jsonObject = new JSONObject();

			if (afterColumnsList.size() != 0) {
				for (CanalEntry.Column column : afterColumnsList) {

					String k = column.getName();
					String v = column.getValue();

					String value = k + "-> " + v;
					System.out.println(value);

					jsonObject.put(k, v);
					jsonObject.toJSONString();

				}
				// 发送到kafka
				MyKafkaSender.sender(topic, jsonObject.toJSONString());

				long random = new Random().nextInt(3);

				try {
					Thread.sleep(random*1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}
	}


}
