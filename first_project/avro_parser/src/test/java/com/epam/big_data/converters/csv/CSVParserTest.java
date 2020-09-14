package com.epam.big_data.converters.csv;

import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class CSVParserTest {

    @Test
    public void readAllOnEmptyString() throws IOException {
        String csvString = "";
        Reader reader = new StringReader(csvString);
        List<String[]> result = CSVParser.readAll(reader);
        List<String[]> expected = new LinkedList<>();
        assertArrayEquals(expected.toArray(), result.toArray());
        reader.close();
    }

    @Test
    public void readAllOnCorrectCSVString() throws IOException {
        String csvString = "colA, ColB\n" +
                "A, B\n" +
                "C, D\n" +
                "G, G\n" +
                "G, F";
        Reader reader = new StringReader(csvString);
        List<String[]> result = CSVParser.readAll(reader);
        List<String[]> expected = Arrays.asList(new String[][] {
                {"colA", " ColB"},
                {"A", " B"},
                {"C", " D"},
                {"G", " G"},
                {"G", " F"},
        });
        assertArrayEquals(expected.toArray(), result.toArray());
        reader.close();
    }

}
