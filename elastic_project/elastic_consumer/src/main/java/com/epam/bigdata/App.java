package com.epam.bigdata;

import com.epam.bigdata.model.Hotel;
import com.epam.bigdata.serializer.JavaDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class App {

    private static Map<String, Object> getKafkaProps() {
        Map<String, Object> consumerConfig = new HashMap();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JavaDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return consumerConfig;
    }

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("elastic_consumer");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaDStream<Hotel> stream =
                KafkaUtils.<byte[], Hotel>createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferBrokers(),
                        ConsumerStrategies.Subscribe(Arrays.asList("streaming"), getKafkaProps())
                ).map(ConsumerRecord::value);

        stream.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}