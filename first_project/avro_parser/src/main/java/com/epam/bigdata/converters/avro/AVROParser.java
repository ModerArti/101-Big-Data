package com.epam.bigdata.converters.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Class for parsing text data to AVRO format
 *
 * @author Arthur_Dorozhkin
 * @version 1.0
 */
public class AVROParser {

    private static final Logger LOGGER = LogManager.getLogger();

    private static Schema generateSchema(List<String[]> strings) {
        String[] headers = strings.remove(0);
        SchemaBuilder.FieldAssembler<Schema> fieldBuilder = SchemaBuilder.record("data")
                .namespace("com.epam.big_data")
                .fields();
        for (String header : headers) {
            fieldBuilder.requiredString(header);
        }
        return fieldBuilder.endRecord();
    }

    private static List<GenericRecord> getDatums(List<String[]> strings, Schema schema) {
        List<GenericRecord> datums = new LinkedList<>();
        for (String[] values : strings) {
            GenericRecord datum = new GenericData.Record(schema);
            for (int i = 0; i < values.length; ++i) {
                datum.put(schema.getFields().get(i).name(), values[i]);
            }
            datums.add(datum);
        }
        return datums;
    }

    /**
     * Method that gets list of strings arrays with data and output stream where data will be writing
     * @param strings List of strings arrays with data
     * @param output Output stream for writing the data
     */
    public static void writeAll(List<String[]> strings, OutputStream output) throws IOException {
        LOGGER.debug("Starts creating the schema");
        Schema schema = generateSchema(strings);
        LOGGER.debug("Ends creating the schema");

        LOGGER.debug("Starts getting datums");
        List<GenericRecord> datums = getDatums(strings, schema);
        LOGGER.debug("Ends getting datums");

        LOGGER.info("Starts writing the AVRO file");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, output);
            for (GenericRecord datum : datums) {
                dataFileWriter.append(datum);
            }
            LOGGER.info("Ends writing the AVRO file");
        } catch (IOException e) {
            LOGGER.error("Some problem with writing AVRO file", e);
            throw e;
        }
    }
}
