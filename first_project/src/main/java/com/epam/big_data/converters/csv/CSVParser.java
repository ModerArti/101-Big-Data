package com.epam.big_data.converters.csv;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;

public class CSVParser {

     private final static Logger LOGGER = LogManager.getLogger();

     public static List<String[]> readAll(Reader reader) {
          LOGGER.info("Starts reading the CSV file");
          try (CSVReader csvReader = new CSVReader(reader)) {
               List<String[]> strings =  csvReader.readAll();
               LOGGER.info("Gets the end of the file");
               return strings;
          } catch (IOException | CsvException e) {
               LOGGER.error("Stops reading the CSV file", e);
               return new LinkedList<>();
          }
     }

}
