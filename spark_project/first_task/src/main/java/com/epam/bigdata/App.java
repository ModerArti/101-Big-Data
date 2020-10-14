package com.epam.bigdata;

import com.epam.bigdata.key.Hotel;
import com.epam.bigdata.sparkhandler.RDDHandler;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class App {

    private static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf().setAppName("FirstApp").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

    public static void main(String[] args) {
        JavaSparkContext sc = getSparkContext();

        RDDHandler rddHandler = new RDDHandler(sc);

        Map<Hotel, Long> hotelToNumberOfReferences = rddHandler.getData("test.csv");

        hotelToNumberOfReferences
                .entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(3)
                .collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new
                ))
                .forEach((key, value) -> {
                    System.out.println(key);
                    System.out.println("Number of references: " + value);
                });

        sc.stop();
    }

}
