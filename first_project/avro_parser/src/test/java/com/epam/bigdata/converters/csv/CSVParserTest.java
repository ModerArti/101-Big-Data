package com.epam.bigdata.converters.csv;

import com.opencsv.exceptions.CsvException;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class CSVParserTest {

    @Test
    public void readAllOnEmptyString() throws IOException, CsvException {
        String csvString = "";
        InputStream in = new BufferedInputStream(new ByteArrayInputStream(csvString.getBytes()));
        List<String[]> result = CSVParser.readAll(in);
        List<String[]> expected = new LinkedList<>();
        assertArrayEquals(expected.toArray(), result.toArray());
        in.close();
    }

    @Test
    public void readAllOnCorrectCSVString() throws IOException, CsvException {
        String csvString = "colA, ColB\n" +
                "A, B\n" +
                "C, D\n" +
                "G, G\n" +
                "G, F";
        InputStream in = new BufferedInputStream(new ByteArrayInputStream(csvString.getBytes()));
        List<String[]> result = CSVParser.readAll(in);
        List<String[]> expected = Arrays.asList(new String[][]{
                {"colA", " ColB"},
                {"A", " B"},
                {"C", " D"},
                {"G", " G"},
                {"G", " F"},
        });
        assertArrayEquals(expected.toArray(), result.toArray());
        in.close();
    }

}
