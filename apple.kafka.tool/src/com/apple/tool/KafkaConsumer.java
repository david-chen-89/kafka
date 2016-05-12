package com.apple.tool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Text;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer implements Runnable {

	private String zookeeperConn;
	private String topic;
	private String groupId;
	private Text text;
	private ProgressBar progressBar;
	private Button btnStartConsumer;
	private ConsumerConnector consumer;

	public KafkaConsumer(String zookeeperConn, String topic, String groupId, Text text, ProgressBar progressBar,
			Button btnStartConsumer) {
		this.zookeeperConn = zookeeperConn;
		this.topic = topic;
		this.groupId = groupId;
		this.text = text;
		this.progressBar = progressBar;
		this.btnStartConsumer = btnStartConsumer;
	}

	private ConsumerConfig createConsumerConfig(String zookeeperConn) {
		final Properties props = new Properties();
		props.put("zookeeper.connect", zookeeperConn);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "1000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		// props.put("client.id",
		// AppleKafkaUtil.generateClientId((String)applicationProperties.get("ds.appId"),
		// "uniqueId"));
		props.put("secure", "false");
		return new ConsumerConfig(props);
	}

	@Override
	public void run() {
		try {
			final ConsumerConfig cfg = createConsumerConfig(zookeeperConn);
			consumer = Consumer.createJavaConsumerConnector(cfg);
			final Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(topic, 1);
			final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
					.createMessageStreams(topicCountMap);
			final KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
			final ConsumerIterator<byte[], byte[]> it = stream.iterator();
			Display.getDefault().asyncExec(new Runnable() {

				@Override
				public void run() {
					progressBar.setSelection(100);
					btnStartConsumer.setText("stop");
					btnStartConsumer.setEnabled(true);
				}
			});
			while (it.hasNext()) {
				try {
					final String message = new String(it.next().message(), "UTF8");
					Display.getDefault().asyncExec(new Runnable() {
						public void run() {
							if (text.getText() == null || "".equals(text.getText())) {
								text.setText(message);
							} else {
								text.setText(text.getText() + "\n\n" + message);
							}
						}
					});
				} catch (final Exception e) {
				}
			}

		} catch (final Exception e) {
			Display.getDefault().asyncExec(new Runnable() {

				@Override
				public void run() {
					MessageBox dialog = new MessageBox(text.getShell(), SWT.ICON_ERROR | SWT.OK);
					dialog.setText("error");
					dialog.setMessage(e.getMessage());
					dialog.open();
				}
			});

		} finally {
			Display.getDefault().asyncExec(new Runnable() {

				@Override
				public void run() {
					progressBar.setSelection(0);
					btnStartConsumer.setText("start");
					btnStartConsumer.setEnabled(true);
				}
			});
		}

	}

	public void stop() {
		if (consumer != null) {
			consumer.shutdown();
		}
	}
}
