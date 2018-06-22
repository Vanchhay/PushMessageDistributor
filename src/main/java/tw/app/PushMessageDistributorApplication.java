package tw.app;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

public class PushMessageDistributorApplication {

	final static String consumerPropsFile = "consumer.properties";
	private final static Logger LOGGER = LoggerFactory.getLogger(PushMessageDistributorApplication.class);

	private static String TOPIC;
	private static InputStream inputStream;
	private static Properties properties = new Properties();

	static {
		try {
			inputStream = PushMessageDistributorApplication.class.getClassLoader().getResourceAsStream(consumerPropsFile);
			properties.load(inputStream);
			// Set TOPIC
			TOPIC = properties.getProperty("TOPIC");
			// Close inputStream
			inputStream.close();

			// Set ConsumerConfig from consumer properties
			properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("BOOTSTRAP_SERVERS"));
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("GROUP_ID"));
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private static Consumer<Long, String> consumer = new KafkaConsumer<>(properties);

	public static void main(String[] args) {
		runConsumer();
	}

	static void runConsumer() {
		// Subcribe Topic to consumer
		consumer.subscribe(Collections.singletonList(TOPIC));

		// Listen to consumer for coming message to log
		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(10);
			consumerRecords.forEach(record -> {
				LOGGER.info("Received Message { key: {} , value: {} , partition: {} }", record.key(), record.value(), record.partition());
			});
			consumer.commitAsync();
		}
	}
}
