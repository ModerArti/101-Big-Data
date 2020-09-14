package com.epam.big_data;

import com.epam.big_data.converters.avro.AVROParser;
import com.epam.big_data.converters.csv.CSVParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class App {

    private static final Logger LOGGER = LogManager.getLogger();

    public static void main(String[] args) {
        List<String[]> strings = readAllFromCSV();
        writeAllToAVRO(strings);
    }

    private static List<String[]> readAllFromCSV() {
        try (Reader reader = Files.newBufferedReader(Paths.get(
                "first_project/files/csv/sample_submission.csv"
        ))) {
            return CSVParser.readAll(reader);
        } catch (IOException e) {
            LOGGER.error(e);
        }
        return new LinkedList<>();
    }

    private static void writeAllToAVRO(List<String[]> strings) {
        try (OutputStream output = new FileOutputStream(new File("first_project/files/avro/data.avro"))) {
            AVROParser.writeAll(strings, output);
        } catch (FileNotFoundException e) {
            LOGGER.error("File not found", e);
        } catch (IOException e) {
            LOGGER.error("Some problem with writing", e);
        }
    }

}
