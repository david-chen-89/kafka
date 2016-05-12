package com.apple.tool;

import java.util.Properties;

import com.apple.ist.appeng.shared.kafka.utils.HashPartitioner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaMessageProducer {
	private static Producer<String, String> producer = null;

	public static void init(String brokerList) {
		final Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", SimplePartitioner.class.getName());
		props.put("partitioner.class", HashPartitioner.class.getName());
		// props.put("key.serializer.class", StringEncoder.class.getName());
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	public static void send(String brokerList, String topic, String message, String partition) {
		init(brokerList);
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, partition, message);
		producer.send(data);
		producer.close();
	}
}
