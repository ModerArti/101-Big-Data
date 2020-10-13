package com.epam.bigdata.sparkhandler;

import com.epam.bigdata.key.Hotel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RDDHandler {

    private JavaSparkContext sc;

    public RDDHandler(JavaSparkContext sc) {
        this.sc = sc;
    }

    public Map<Hotel, Long> getData(String pathToFile) {
        JavaRDD<String> text = sc.textFile(pathToFile);

        JavaRDD<String[]> wordsByLine = text.map(line -> line.split(","));
        wordsByLine = wordsByLine.cache();
        List<String> headers = Arrays.asList(wordsByLine.first());
        wordsByLine = wordsByLine.filter(line -> !Arrays.equals(line, headers.toArray()));

        int id = headers.indexOf("id");
        int continentIndex = headers.indexOf("hotel_continent");
        int countryIndex = headers.indexOf("hotel_country");
        int marketIndex = headers.indexOf("hotel_market");

        JavaPairRDD<Hotel, String[]> compositeKeyToLine = wordsByLine.mapToPair(line ->
                new Tuple2<>(new Hotel(
                        line[id], line[continentIndex], line[countryIndex], line[marketIndex]
                ), line));

        return compositeKeyToLine.countByKey();
    }

}
