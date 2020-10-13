package com.epam.bigdata;

import com.epam.bigdata.model.Hotel;
import com.epam.bigdata.serializer.JavaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class App {

    private static Properties getKafkaProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JavaSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private static Hotel generateHotels() {
        Random random = new Random();
        Hotel hotel = new Hotel(
                Integer.toString(random.nextInt(100)),
                Integer.toString(random.nextInt(100)),
                Integer.toString(random.nextInt(100)),
                Integer.toString(random.nextInt(100))
        );
        return hotel;
    }

    public static void main(String[] args) {
        try (Producer<String, Hotel> producer = new KafkaProducer<>(getKafkaProps())) {
            while (true) {
                Hotel hotel = generateHotels();
                ProducerRecord<String, Hotel> data = new ProducerRecord<>(
                        "streaming", hotel);
                producer.send(data);
                producer.flush();
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
