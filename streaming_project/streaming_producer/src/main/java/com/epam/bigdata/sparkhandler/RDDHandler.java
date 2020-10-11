package com.epam.bigdata.sparkhandler;

import com.epam.bigdata.model.Hotel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDHandler {

    private JavaSparkContext sc;

    public RDDHandler(JavaSparkContext sc) {
        this.sc = sc;
    }

    public List<Hotel> getData(String pathToFile) {
        JavaRDD<String> text = sc.textFile(pathToFile);

        JavaRDD<String[]> wordsByLine = text.map(line -> line.split(","));
        List<String> headers = Arrays.asList(wordsByLine.take(1).get(0));
        wordsByLine = wordsByLine.filter(line -> !Arrays.equals(line, headers.toArray()));

        int id = headers.indexOf("id");
        int continentIndex = headers.indexOf("hotel_continent");
        int countryIndex = headers.indexOf("hotel_country");
        int marketIndex = headers.indexOf("hotel_market");

        JavaRDD<Hotel> compositeKeyToLine = wordsByLine.map(line ->
                new Hotel(
                        line[id], line[continentIndex], line[countryIndex], line[marketIndex]
                ));

        return compositeKeyToLine.collect();
    }

}
