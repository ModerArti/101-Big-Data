package com.epam.bigdata;

import com.epam.bigdata.config.Config;
import com.epam.bigdata.converters.avro.AVROParser;
import com.epam.bigdata.converters.csv.CSVParser;
import com.epam.bigdata.hdfs.HDFSConnector;
import com.opencsv.exceptions.CsvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * App for parsing data from CSV to AVRO
 *
 * @author Arthur_Dorozhkin
 * @version 1.0
 */
public class App {

    private static final Logger logger = LogManager.getLogger();

    private static final String PATH_TO_CSV = Config.loadProperty("hdfs.file.csv");
    private static final String PATH_TO_AVRO = Config.loadProperty("hdfs.file.avro");

    public static void main(String[] args) {
        try (InputStream fromHDFStoCSV = HDFSConnector.readFile(PATH_TO_CSV);
             OutputStream fromAVROtoHDFS = HDFSConnector.writeFile(PATH_TO_AVRO)) {
            CSVParser csvParser = new CSVParser(fromHDFStoCSV);
            AVROParser avroParser = new AVROParser(fromAVROtoHDFS);

            logger.info("Start parsing the data");
            String[] strings;
            while ((strings = csvParser.readLine()) != null) {
                avroParser.writeLine(strings);
            }
            logger.info("End parsing the data");
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
    }

}
