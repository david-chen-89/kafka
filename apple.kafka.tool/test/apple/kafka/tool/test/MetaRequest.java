package apple.kafka.tool.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class MetaRequest {

	private static final int CONSUMER_TIMEOUT = 100000;
	private static final int CONSUMER_BUFFER_SIZE = 64 * 1024;

	@Test
	public void test() {
		SimpleConsumer consumer = null;
		try {
			String leadBroker = "ma-curot-lapp50.corp.apple.com";
			int port = 9092; // 2181 9092
			String clientName = "Rule";
			consumer = new SimpleConsumer(leadBroker, port, CONSUMER_TIMEOUT, CONSUMER_BUFFER_SIZE, clientName);

			String topic = "Rule";
			// List<String> topics = Collections.singletonList(topic);
			List<String> topics = new ArrayList<>();
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

			List<TopicMetadata> metaData = resp.topicsMetadata();
			for (TopicMetadata item : metaData) {
				System.out.println("------ start ------");
				System.out.println("topic: " + item.topic());

				for (PartitionMetadata part : item.partitionsMetadata()) {
					System.out.println(part.partitionId());
					System.out.println(part.isr());
					System.out.println(part.leader());
					System.out.println(part.replicas());
				}
				System.out.println("------- end -------");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}
	}
}
