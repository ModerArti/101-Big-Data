package com.epam.bigdata;

import com.epam.bigdata.model.Hotel;
import com.epam.bigdata.serializer.JavaDeserializer;
import com.epam.bigdata.sparkhandler.RDDHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class App {

    private static Properties getKafkaProps() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JavaDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return consumerConfig;
    }

    public static void main(String[] args) {
        RDDHandler rddHandler = new RDDHandler("data");

        KafkaConsumer<byte[], Hotel> consumer = new KafkaConsumer<>(getKafkaProps());
        consumer.subscribe(Collections.singletonList("streaming"));

        while (true) {
            List<Hotel> hotels = new LinkedList<>();
            ConsumerRecords<byte[], Hotel> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<byte[], Hotel> record : records) {
                hotels.add(record.value());
            }

            if (hotels.size() != 0) {
                rddHandler.writeData(hotels);
            }

            consumer.commitSync();
        }

    }

}