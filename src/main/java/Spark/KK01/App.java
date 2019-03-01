package Spark.KK01;

import java.util.*;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App {
	private static final String BOOTSTRAP_SERVER = "10.5.36.64:9092,10.5.36.68:9092,10.5.36.71:9092,10.5.36.78:9092,10.5.36.80:9092,10.5.36.81:9092,10.5.36.83:9092,10.5.36.84:9092,10.5.36.87:9092,10.5.36.91:9092";
	private static final String TOPIC="rt-retarget_banner";
	private static final String GROUP_ID="rtgA";
	
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(50));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVER);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", GROUP_ID);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList(TOPIC);
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		stream.count().print();		
		
//		JavaPairDStream<Integer, Integer> counts = stream
//				.map(cr -> cr.partition())
//				.mapToPair(p -> new Tuple2<>(p, 1))
//				.reduceByKey((a, b) -> a+b);
//		counts.print();
		
		jssc.start();
		jssc.awaitTermination();
	}

}
