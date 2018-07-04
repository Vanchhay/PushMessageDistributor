package tw.app;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class PushMessageDistributorApplication {

	final static String consumerPropsFile = "consumer.properties";
	private final static Logger LOGGER = LoggerFactory.getLogger(PushMessageDistributorApplication.class);

	private static String TOPIC;
	private static InputStream inputStream;
	private static Properties properties = new Properties();

//	private static FileWriter fw ;
//	private static BufferedWriter bw ;

	static final String PROJECT_DIR = System.getProperty("user.dir");
	static final String LOG_DIR = PROJECT_DIR + "\\logPushMessage\\";
	static boolean logDir_exist = new File(LOG_DIR).exists();

	static String openedFile = "";

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
			properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
			properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private static Consumer<Long, String> consumer = new KafkaConsumer<>(properties);

	public static void main(String[] args) {

		// If directory log dir not exist
		if (!logDir_exist) {
			File projectDir = new File(LOG_DIR);
			projectDir.mkdir();
		}
		runConsumer();
	}

	static void runConsumer(){
		List<String> topics = Arrays.asList("topic1","topic2","topic3","topic4","topic5","topic6","topic7","topic8","topic9","topic10");
		// Subscribe Topic to consumer
		consumer.subscribe(topics);

		HashMap<String, BufferedWriter> hmap = new HashMap<>();
		for (String topic: topics ) {
			try{
				hmap.put(topic, new BufferedWriter(new FileWriter(LOG_DIR + topic + ".log", true)));
			}catch(IOException e){
				e.printStackTrace();
			}
		}

		// Listen to consumer for coming message to log
		Gson gson = new Gson();
		PushMessage pm;

		while (true) {

			ConsumerRecords<Long, String> records = consumer.poll(10000);
			for (TopicPartition tp : records.partitions()) {
				List<ConsumerRecord<Long, String>> partRecords = records.records(tp);
				for (ConsumerRecord<Long, String> record : partRecords) {
					pm = gson.fromJson(record.value(), PushMessage.class);

					LOGGER.info("Start {}", pm.getTopic());
					try{
						hmap.get(pm.getTopic()).write("[ " + pm.getSendTime() + " ] => "+ pm.toString());
						hmap.get(pm.getTopic()).newLine();
					}catch(IOException e){
						e.printStackTrace();
					}
					LOGGER.info("Finished {}", pm.getSendTime());
				}
				for (String topic: topics ) {
					try{
						hmap.get(topic).flush();
					}catch(IOException e){
						e.printStackTrace();
					}
				}
				consumer.commitAsync();
			}
		}
	}
}