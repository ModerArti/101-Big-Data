package com.epam.bigdata;

import com.epam.bigdata.model.Hotel;
import com.epam.bigdata.serializer.JavaDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
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

        KafkaConsumer<byte[], Hotel> consumer = new KafkaConsumer<>(getKafkaProps());
        consumer.subscribe(Collections.singletonList("homework"));

        while (true) {
            ConsumerRecords<byte[], Hotel> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<byte[], Hotel> record : records) {
                System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }

            consumer.commitSync();
        }

    }

}