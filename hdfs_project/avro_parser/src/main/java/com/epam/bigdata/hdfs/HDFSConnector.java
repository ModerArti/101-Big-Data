package com.epam.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
    public static InputStream readFile(String pathToFile) throws IOException {
        try {
            FileSystem fs = FileSystem.get(config);
            return fs.open(new Path(pathToFile));
        } catch (IOException e) {
            logger.error("Can't load file for reading", e);
            throw e;
        }
    }

    /**
     * Method for writing files
     *
     * @return <code>OutputStream</code> for further data processing
     */
    public static OutputStream writeFile(String pathToFile) throws IOException {
        try {
            FileSystem fs = FileSystem.get(config);
            return fs.create(new Path(pathToFile));
        } catch (IOException e) {
            logger.error("Can't load file for writing", e);
            throw e;
        }
    }

}
