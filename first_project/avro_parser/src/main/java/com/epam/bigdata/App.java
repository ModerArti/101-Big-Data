package com.epam.bigdata;

import com.epam.bigdata.converters.avro.AVROParser;
import com.epam.bigdata.converters.csv.CSVParser;
import com.epam.bigdata.hdfs.HDFSConnector;
import com.opencsv.exceptions.CsvException;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;

/**
 * App for parsing data from CSV to AVRO
 *
 * @author Arthur_Dorozhkin
 * @version 1.0
 */
public class App {

    public static void main(String[] args) throws IOException, CsvException {
        PipedOutputStream outFromHDFStoCSV = HDFSConnector.readFile();
        List<String[]> strings = readAllFromCSV(outFromHDFStoCSV);
        PipedOutputStream outFromAVROToHDFS = writeAllToAVRO(strings);
        HDFSConnector.writeFile(new PipedInputStream(outFromAVROToHDFS));
    }

    private static List<String[]> readAllFromCSV(PipedOutputStream out) throws IOException, CsvException {
        try (PipedInputStream in = new PipedInputStream(out)) {
            return CSVParser.readAll(in);
        }
    }

    private static PipedOutputStream writeAllToAVRO(List<String[]> strings) throws IOException {
        PipedOutputStream output = new PipedOutputStream();
        AVROParser.writeAll(strings, output);
        return output;
    }

}
