package com.epam.big_data;

import com.epam.big_data.converters.csv.CSVParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class App {

    private static final Logger LOGGER = LogManager.getLogger();

    public static void main(String[] args) {
        try {
            List<String[]> strings = readAll();
            writeAll(strings);
        } catch (IOException e) {
            LOGGER.error(e);
        }
    }

    private static List<String[]> readAll() throws IOException {
        Reader reader = Files.newBufferedReader(Paths.get(
                "first_project/files/csv/sample_submission.csv"
        ));
        return CSVParser.readAll(reader);
    }

    private static void writeAll(List<String[]> strings) throws IOException {
        Writer writer = Files.newBufferedWriter(Paths.get(
                "first_project/files/csv/sample_submission.txt"
        ));
        for (String[] stringArray : strings) {
            for (int i = 0; i < stringArray.length; ++i) {
                writer.write(stringArray[i]);
                if (i != stringArray.length - 1) {
                    writer.write(", ");
                }
            }
            writer.write("\n");
        }
    }

}
