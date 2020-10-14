package com.epam.bigdata.sparkhandler;

import com.epam.bigdata.model.Hotel;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class RDDHandler {

    private JavaSparkContext sc;
    private final String PATH_TO_FILE;

    public RDDHandler(JavaSparkContext sc, String pathToFile) {
        this.sc = sc;
        this.PATH_TO_FILE = pathToFile;
    }

    public void writeData(List<Hotel> hotels) {
        //JavaRDD<Hotel> hotelsInRDD = sc.parallelize(hotels);
        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("streaming_consumer")
                .getOrCreate();
        Dataset<Row> dataframe = session
                .createDataFrame(hotels, Hotel.class);

        dataframe.write()
                .mode(SaveMode.Append)
                .csv(PATH_TO_FILE);
    }

}
