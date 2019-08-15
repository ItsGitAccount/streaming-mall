package com.atguigu.streamingmall.canal.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaSender {

	public static KafkaProducer<String, String> kafkaProducer = null;


	//创建kafka生产者
	public static KafkaProducer<String, String> createKafkaProducer() {

		Properties prop = new Properties();
		prop.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		try {
			kafkaProducer = new KafkaProducer<>(prop);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return kafkaProducer;

	}


	//像kafka发送消息
	public static void sender(String topic, String value) {

		if (kafkaProducer == null) {
			kafkaProducer = createKafkaProducer();
		}

		kafkaProducer.send(new ProducerRecord<>(topic, value));
	}

}

/**
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 */