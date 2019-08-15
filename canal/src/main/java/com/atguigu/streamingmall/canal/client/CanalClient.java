package com.atguigu.streamingmall.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.streamingmall.canal.util.CanalHandler;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {


	public static void main(String[] args) {
		new CanalClient().watch("hadoop102",11111,"example","streaming_mall.*");
	}


	public void watch(String hostname, int port, String destination, String tables) {

		CanalConnector canalConnector = CanalConnectors.newSingleConnector(
				new InetSocketAddress(hostname, port),
				destination,
				"",
				""
		);

		// message => entry => rowChange => rowDataList => rowData => columnDataList => columnData => columnName & columnValue

		while (true) {

			// 获取连接
			canalConnector.connect();
			// 订阅表
			canalConnector.subscribe(tables);
			// 每个entry包含的sql数目
			Message message = canalConnector.get(100);
			// 从message获取entry
			List<CanalEntry.Entry> entries = message.getEntries();

			if (entries.size() == 0) {
				System.out.println("没有数据，休息一下吧！");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				for (CanalEntry.Entry entry : entries) {
					if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

						CanalEntry.RowChange rowChange = null;
						try {
							// 得到rowChange
							rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
						} catch (InvalidProtocolBufferException e) {
							e.printStackTrace();
						}

						// 表信息，操作类型，行数据
						String tableName = entry.getHeader().getTableName();
						CanalEntry.EventType eventType = rowChange.getEventType();
						List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

						// 调用工具类，进行业务操作
						CanalHandler.handle(tableName, eventType, rowDatasList);
					}

				}
			}

		}


	}
}
