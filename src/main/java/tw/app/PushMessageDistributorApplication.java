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
	private static Consumer<Long, String> consumer;

	static {
		try {
			Properties properties = new Properties();
			try{
				inputStream = PushMessageDistributorApplication.class.getClassLoader().getResourceAsStream(consumerPropsFile);
				properties.load(inputStream);
				TOPIC = properties.getProperty("TOPIC");
			}catch(IOException e){
				e.printStackTrace();
			}finally {
				inputStream.close();
			}
			properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("BOOTSTRAP_SERVERS"));
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("GROUP_ID"));
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			consumer = new KafkaConsumer<>(properties);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		LOGGER.info("===========   Main is running, calling runConsumer()  ================");
		runConsumer();
		LOGGER.info("===========  Main execution done  ================ ");
	}

	static void runConsumer() {

		final int giveUp = 100;
		int noRecordsCount = 0;

		LOGGER.info(" Subcribing TOPIC  ");
		consumer.subscribe(Collections.singletonList(TOPIC));
		LOGGER.info("/////////// Subcribed   //////////////////////");
		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

			LOGGER.info("consumerRecords.count = {} ", consumerRecords.count());
			if (consumerRecords.count()==0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp) break;
				else continue;
			}

			LOGGER.info("===== foreach ===== ");
			consumerRecords.forEach(record -> {
				LOGGER.info("Received Message { key: {} , value: {} , partition: {} ", record.key(), record.value(), record.partition());
			});

			consumer.commitAsync();
		}
		consumer.close();
	}
}
