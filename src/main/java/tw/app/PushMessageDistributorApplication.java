package tw.app;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;

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
			Properties propsInFile = new Properties();
			inputStream = PushMessageDistributorApplication.class.getClassLoader().getResourceAsStream(consumerPropsFile);
			propsInFile.load(inputStream);
			// Set TOPIC
			TOPIC = propsInFile.getProperty("TOPIC");
			// Close inputStream
			inputStream.close();

			// Set ConsumerConfig from consumer properties
			properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, propsInFile.getProperty("BOOTSTRAP_SERVERS"));
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, propsInFile.getProperty("GROUP_ID"));
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
		Gson gson = new Gson();
		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(10);
			PushMessage pm;
			for (ConsumerRecord<Long, String> record : consumerRecords) {
					pm = gson.fromJson(record.value(), PushMessage.class);
					LOGGER.info("[{}] - PushMessage : Sender: {} , Topic: {} , Urgent: {} , Text: {}",
							pm.getSendTime(), pm.getSender(), pm.getTopic(), pm.getUrgent(), pm.getText());
			}
			consumer.commitAsync();
		}
	}
}