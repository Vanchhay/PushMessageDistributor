package tw.app;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;

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

		try{
			runConsumer();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	static void runConsumer() throws IOException {
		List<String> topics = Arrays.asList("topic1","topic2","topic3","topic4","topic5","topic6","topic7","topic8","topic9","topic10");
		// Subscribe Topic to consumer
		consumer.subscribe(topics);

		FileWriter f1=null; FileWriter f5=null; FileWriter f8=null;
		FileWriter f2=null; FileWriter f6=null; FileWriter f9=null;
		FileWriter f3=null; FileWriter f7=null; FileWriter f10=null;
		FileWriter f4=null;

		f1 = new FileWriter(LOG_DIR + "topic1.log", true);
		f2 = new FileWriter(LOG_DIR + "topic2.log", true);
		f3 = new FileWriter(LOG_DIR + "topic3.log", true);
		f4 = new FileWriter(LOG_DIR + "topic4.log", true);
		f5 = new FileWriter(LOG_DIR + "topic5.log", true);
		f6 = new FileWriter(LOG_DIR + "topic6.log", true);
		f7 = new FileWriter(LOG_DIR + "topic7.log", true);
		f8 = new FileWriter(LOG_DIR + "topic8.log", true);
		f9 = new FileWriter(LOG_DIR + "topic9.log", true);
		f10 = new FileWriter(LOG_DIR + "topic10.log", true);

		// Listen to consumer for coming message to log
		Gson gson = new Gson();
		PushMessage pm;
		BufferedWriter bw = null;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
			for (ConsumerRecord<Long, String> record : consumerRecords) {
				pm = gson.fromJson(record.value(), PushMessage.class);
//				LOGGER.info(""+record.partition());

				switch (pm.topic) {
					case "topic1":
						bw = new BufferedWriter(f1);
						break;
					case "topic2":
						bw = new BufferedWriter(f2);
						break;
					case "topic3":
						bw = new BufferedWriter(f3);
						break;
					case "topic4":
						bw = new BufferedWriter(f4);
						break;
					case "topic5":
						bw = new BufferedWriter(f5);
						break;
					case "topic6":
						bw = new BufferedWriter(f6);
						break;
					case "topic7":
						bw = new BufferedWriter(f7);
						break;
					case "topic8":
						bw = new BufferedWriter(f8);
						break;
					case "topic9":
						bw = new BufferedWriter(f9);
						break;
					case "topic10":
						bw = new BufferedWriter(f10);
						break;
				}
				LOGGER.info("Start {}", pm.getTopic());
				bw.write("[ " + pm.getSendTime() + " ] => "+ pm.toString());
				bw.newLine();
				bw.flush();
				LOGGER.info("FInished {}", pm.getSendTime());
			}
			consumer.commitAsync();
		}
	}

	static void writeToReadableFile(PushMessage pm) throws IOException{

//		final String fileName = LOG_DIR + pm.getTopic().trim().toLowerCase() + ".log";

//		fw =
//		bw = new BufferedWriter(new FileWriter(LOG_DIR + pm.getTopic().trim().toLowerCase() + ".log", true));
//
//		bw.write("[ " + pm.getSendTime() + " ] => "+ pm.toString());
//		bw.newLine();
//
//		bw.flush();
	}
}
