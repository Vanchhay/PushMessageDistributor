package tw.app;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class PushMessageDistributorApplication {

	private final static String TOPIC = "distributor";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static Logger LOGGER = LoggerFactory.getLogger(PushMessageDistributorApplication.class);

	private static Properties propsConsumer = setConsumerProps();
	private static Consumer<Long, String> consumer = new KafkaConsumer<>(propsConsumer);

	private static Properties setConsumerProps(){
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Distributor");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return properties;
	}



//	private static Consumer<Long, String> createConsumer() {
//		// Subscribe to the topic.
//		consumer.subscribe(Collections.singletonList(TOPIC));
//		return consumer;
//	}
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
