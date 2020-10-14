package com.epam.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        dataframe.createOrReplaceTempView("table");

        spark.sql("SELECT hotel_country, COUNT(hotel_country) FROM table " +
                        "WHERE srch_children_cnt != 0 " +
                "AND is_mobile = 0 " +
                "GROUP BY hotel_country " +
                "ORDER BY COUNT(hotel_country) DESC " +
                "LIMIT 1;").show();

    }

}
