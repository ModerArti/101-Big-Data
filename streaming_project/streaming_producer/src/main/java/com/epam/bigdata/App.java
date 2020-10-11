package com.epam.bigdata;

import com.epam.bigdata.model.Hotel;
import com.epam.bigdata.serializer.JavaSerializer;
import com.epam.bigdata.sparkhandler.RDDHandler;
import org.apache.kafka.clients.producer.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Properties;

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

    private static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf().setAppName("Producer").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

    public static void main(String[] args){
        Producer<String, Hotel> producer = new KafkaProducer<>(getKafkaProps());

        JavaSparkContext sc = getSparkContext();

        RDDHandler rddHandler = new RDDHandler(sc);

        List<Hotel> hotels = rddHandler.getData("test.csv");

        TestCallback callback = new TestCallback();
        for (Hotel hotel : hotels) {
            ProducerRecord<String, Hotel> data = new ProducerRecord<>(
                    "homework", hotel);
            producer.send(data, callback);
            producer.flush();
        }

        producer.close();
    }


    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }

}
