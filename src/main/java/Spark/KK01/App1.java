package Spark.KK01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.KafkaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
//import java.util.Set;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
//import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
//import java.util.Collections;
//import java.util.Properties;
import org.apache.spark.streaming.Durations;

public class App1 {
	private static final String BOOTSTRAP_SERVER = "10.5.36.64:9092,10.5.36.68:9092,10.5.36.71:9092,10.5.36.78:9092,10.5.36.80:9092,10.5.36.81:9092,10.5.36.83:9092,10.5.36.84:9092,10.5.36.87:9092,10.5.36.91:9092";
	private static final String TOPIC = "rt-retarget_banner";
	private static final String GROUP_ID = "rtgA";

	private static Consumer<String, String> createConsumer() {
//		final Properties props = new Properties();
//		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
//		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVER);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", GROUP_ID);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		final Consumer<String, String> consumer = new KafkaConsumer<>(kafkaParams);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC));
		return consumer;
	}

	public static void main(String args[]) throws InterruptedException {
		//SparkConf conf = new SparkConf();
		//JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(30));
		
		Consumer<String, String> consumer = createConsumer();
		int giveUp = 100;
		int noRecordsCount = 0;
		//boolean key = true;
		long count = 0;
		long time1 = System.currentTimeMillis(), time2 = time1;
		while (true) {
			time2 = System.currentTimeMillis();
			if (time2-time1>=50000) {
				System.out.println(time1+" - Count: " + count);
				count = 0;
				time1 = time2;
			}
			ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			}
			count = count + consumerRecords.count();
			
//			consumerRecords.forEach(record -> {
//				System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
//						record.partition(), record.offset());
//			
//			});
			consumer.commitAsync();
		}
		consumer.close();
		//jssc.start();
		//jssc.awaitTerminationOrTimeout(100000);
	}
}
