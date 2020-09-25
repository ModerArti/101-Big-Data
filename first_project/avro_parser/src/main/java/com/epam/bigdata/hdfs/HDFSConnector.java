package com.epam.bigdata.hdfs;

import com.epam.bigdata.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URI;

/**
 * Class for connecting to HDFS for reading and writing files
 *
 * @author Arthur_Dorozhkin
 * @version 1.0
 */
public class HDFSConnector {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String HDFS_URL;
    private static final Configuration config = new Configuration(false);

    static {
        config.addResource(new Path(
                Config.loadProperty("hdfs.core-site.file")
        ));
        config.addResource(new Path(
                Config.loadProperty("hdfs.hdfs-site.file")
        ));
        HDFS_URL = config.get("fs.defaultFS");
    }

    /**
     * Method for reading files
     *
     * @return <code>PipedOutputStream</code> for further data processing
     */
    public static PipedOutputStream readFile() throws IOException {
        InputStream in = null;
        try {
            final String PATH_TO_FILE = Config.loadProperty("hdfs.file.csv");
            final String uri = HDFS_URL + PATH_TO_FILE;
            FileSystem fs = FileSystem.get(URI.create(uri), config);
            PipedOutputStream out = new PipedOutputStream();
            in = fs.open(new Path(uri));
            LOGGER.info("Starts reading file from HDFS");
            IOUtils.copyBytes(in, out, config, false);
            LOGGER.info("Ends reading file from HDFS");
            return out;
        } catch (IOException e) {
            LOGGER.error("Can't load file for reading", e);
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
            final String uri = HDFS_URL + PATH_TO_FILE;
            FileSystem fs = FileSystem.get(URI.create(uri), config);
            OutputStream out = fs.append(new Path(uri));
            LOGGER.info("Starts writing file to HDFS");
            IOUtils.copyBytes(in, out, config, true);
            LOGGER.info("Ends writing file to HDFS");
        } catch (IOException e) {
            LOGGER.error("Can't load file for writing", e);
            throw e;
        }
    }

}
