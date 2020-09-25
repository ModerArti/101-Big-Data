package com.epam.bigdata.converters.csv;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Class for parsing CSV files
 *
 * @author Arthur_Dorozhkin
 * @version 1.0
 */
public class CSVParser {

     private final static Logger LOGGER = LogManager.getLogger();

     /**
      * Method that gets the <code>reader</code> and read CSV data from it to list of strings arrays
      * @param reader Source of CSV data
      * @return List of strings arrays with parsed data
      */
     public static List<String[]> readAll(InputStream reader) throws IOException, CsvException {
          LOGGER.info("Starts reading the CSV file");
          try (CSVReader csvReader = new CSVReader(new InputStreamReader(reader))) {
               List<String[]> strings =  csvReader.readAll();
               LOGGER.info("Gets the end of the file");
               return strings;
          } catch (IOException | CsvException e) {
               LOGGER.error("Can't read the CSV file", e);
               throw e;
          }
     }

}
