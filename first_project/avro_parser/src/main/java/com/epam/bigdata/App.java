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
import java.util.List;

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

    public static void main(String[] args) throws IOException, CsvException {
        InputStream fromHDFStoCSV = HDFSConnector.readFile(PATH_TO_CSV);

        List<String[]> strings = readAllFromCSV(fromHDFStoCSV);

        if (!strings.isEmpty()) {
            OutputStream fromAVROtoHDFS = HDFSConnector.writeFile(PATH_TO_AVRO);

            writeAllToAVRO(fromAVROtoHDFS, strings);
        } else {
            logger.warn("CSV file is empty");
        }
    }

    private static List<String[]> readAllFromCSV(InputStream in) throws IOException, CsvException {
        return CSVParser.readAll(in);
    }

    private static void writeAllToAVRO(OutputStream out, List<String[]> strings) throws IOException {
        AVROParser.writeAll(out, strings);
    }

}
