package com.epam.big_data;

import com.epam.big_data.converters.avro.AVROParser;
import com.epam.big_data.converters.csv.CSVParser;
import com.epam.big_data.hdfs.HDFSConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * App for parsing data from CSV to AVRO
 *
 * @author Arthur_Dorozhkin
 * @version 1.0
 */
public class App {

    private static final Logger LOGGER = LogManager.getLogger();

    public static void main(String[] args) throws IOException {
        PipedOutputStream outFromHDFStoCSV = HDFSConnector.readFile();
        List<String[]> strings = readAllFromCSV(outFromHDFStoCSV);
        PipedOutputStream outFromAVROToHDFS = writeAllToAVRO(strings);
        HDFSConnector.writeFile(new PipedInputStream(outFromAVROToHDFS));
    }

    private static List<String[]> readAllFromCSV(PipedOutputStream out) {
        try (PipedInputStream in = new PipedInputStream(out)) {
            return CSVParser.readAll(in);
        } catch (IOException e) {
            LOGGER.error(e);
        }
        return new LinkedList<>();
    }

    private static PipedOutputStream writeAllToAVRO(List<String[]> strings) {
        PipedOutputStream output = new PipedOutputStream();
        AVROParser.writeAll(strings, output);
        return output;
    }

}
