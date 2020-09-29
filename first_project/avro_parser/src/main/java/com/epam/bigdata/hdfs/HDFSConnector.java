package com.epam.bigdata.hdfs;

import com.epam.bigdata.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedOutputStream;

/**
 * Class for connecting to HDFS for reading and writing files
 *
 * @author Arthur_Dorozhkin
 * @version 1.0
 */
public class HDFSConnector {

    private static final Logger logger = LogManager.getLogger();

    private static final Configuration config = new Configuration();

    /**
     * Method for reading files
     *
     * @return <code>PipedOutputStream</code> for further data processing
     */
    public static PipedOutputStream readFile() throws IOException {
        InputStream in = null;
        try {
            final String PATH_TO_FILE = Config.loadProperty("hdfs.file.csv");
            FileSystem fs = FileSystem.get(config);
            PipedOutputStream out = new PipedOutputStream();
            in = fs.open(new Path(PATH_TO_FILE));
            logger.info("Starts reading file from HDFS");
            IOUtils.copyBytes(in, out, config, false);
            logger.info("Ends reading file from HDFS");
            return out;
        } catch (IOException e) {
            logger.error("Can't load file for reading", e);
            throw e;
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * Method for writing files
     *
     * @param in <code>InputStream</code> with data
     */
    public static void writeFile(InputStream in) throws IOException {
        try {
            final String PATH_TO_FILE = Config.loadProperty("hdfs.file.avro");
            FileSystem fs = FileSystem.get(config);
            OutputStream out = fs.append(new Path(PATH_TO_FILE));
            logger.info("Starts writing file to HDFS");
            IOUtils.copyBytes(in, out, config, true);
            logger.info("Ends writing file to HDFS");
        } catch (IOException e) {
            logger.error("Can't load file for writing", e);
            throw e;
        }
    }

}
