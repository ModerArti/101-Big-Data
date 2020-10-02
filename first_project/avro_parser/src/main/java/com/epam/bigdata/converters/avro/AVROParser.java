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

    private final Logger logger = LogManager.getLogger();

    private Schema schema;
    private OutputStream out;

    /**
     * Init OutputStream in AVROParser
     * @param outputStream output stream for writing the data
     */
    public AVROParser(OutputStream outputStream) {
        this.out = outputStream;
    }

    private Schema generateSchema(String[] headers) {
        SchemaBuilder.FieldAssembler<Schema> fieldBuilder = SchemaBuilder.record("data")
                .namespace("com.epam.bigdata")
                .fields();
        for (String header : headers) {
            fieldBuilder.requiredString(header);
        }
        return fieldBuilder.endRecord();
    }

    private List<GenericRecord> getDatums(String[] values, Schema schema) {
        List<GenericRecord> datums = new LinkedList<>();
        GenericRecord datum = new GenericData.Record(schema);
        for (int i = 0; i < values.length; ++i) {
            datum.put(schema.getFields().get(i).name(), values[i]);
        }
        datums.add(datum);
        return datums;
    }

    /**
     * Method that gets list of strings arrays with data and writing it
     *
     * @param strings List of strings arrays with data
     */
    public void writeLine(String[] strings) throws IOException {
        if (schema == null) {
            logger.debug("Start creating the schema");
            schema = generateSchema(strings);
            logger.debug("End creating the schema");
        } else {
            logger.debug("Start getting datums");
            List<GenericRecord> datums = getDatums(strings, schema);
            logger.debug("End getting datums");

            logger.debug("Writing line to AVRO file");
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                dataFileWriter.create(schema, out);
                for (GenericRecord datum : datums) {
                    dataFileWriter.append(datum);
                }
                logger.debug("End writing line to AVRO file");
            } catch (IOException e) {
                logger.error("Some problem with writing line to AVRO file", e);
                throw e;
            }
        }
    }
}
