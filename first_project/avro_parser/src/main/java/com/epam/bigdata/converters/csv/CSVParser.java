package com.epam.bigdata.converters.csv;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Class for parsing CSV files
 *
 * @author Arthur_Dorozhkin
 * @version 1.0
 */
public class CSVParser {

    private final Logger logger = LogManager.getLogger();

    private CSVReader csvReader;

    /**
     * Method for init CSVReader
     * @param inputStream input stream with data
     */
    public CSVParser(InputStream inputStream) {
        this.csvReader = new CSVReader(new InputStreamReader(inputStream));
    }

    /**
     * Method that read one line of CSV data from it to strings arrays
     *
     * @return List of strings arrays with parsed data
     */
    public String[] readLine() throws IOException, CsvException {
        logger.debug("Start reading line from the CSV file");
        try {
            String[] strings = csvReader.readNext();
            logger.debug("End reading line from the CSV file");
            return strings;
        } catch (IOException | CsvException e) {
            logger.error("Can't read the CSV file", e);
            throw e;
        }
    }

}
