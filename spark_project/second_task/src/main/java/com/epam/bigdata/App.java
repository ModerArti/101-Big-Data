package com.epam.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.desc;

public class App {

    private static SparkSession initSparkSession() {
        return SparkSession.builder()
                .master("local[*]")
                .appName("SecondTask")
                .getOrCreate();
    }

    private static Dataset<Row> initDataframe(SparkSession spark) {
        return spark.read()
                .option("header", true)
                .csv("test.csv");
    }

    public static void main(String[] args) {
        SparkSession spark = initSparkSession();

        Dataset<Row> dataframe = initDataframe(spark);

        dataframe.select(
                dataframe.col("hotel_country")
        ).where(
                dataframe.col("hotel_country")
                        .equalTo(
                                dataframe.col("user_location_country")
                        )
                        .and(
                                dataframe.col("is_mobile")
                                        .equalTo("1")
                        )
        ).groupBy(
                dataframe.col("hotel_country")
        ).count()
                .sort(desc("count"))
                .show(1);

        spark.stop();
    }

}
