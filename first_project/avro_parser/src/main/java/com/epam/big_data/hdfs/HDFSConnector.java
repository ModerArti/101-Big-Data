package com.epam.big_data.hdfs;

import com.epam.big_data.config.Config;
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

    private static final String HDFS_URL = Config.loadProperty("hdfs.url");
    private static final Configuration config = new Configuration(false);

    /**
     * Method for reading files
     *
     * @return <code>PipedOutputStream</code> for further data processing
     */
    public static PipedOutputStream readFile() {
        InputStream in = null;
        PipedOutputStream out = new PipedOutputStream();
        try {
            config.set("fs.defaultFS", Config.loadProperty("hdfs.url"));
            config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            final String PATH_TO_FILE = Config.loadProperty("hdfs.file.csv");
            final String uri = HDFS_URL + PATH_TO_FILE;
            FileSystem fs = FileSystem.get(URI.create(uri), config);
            in = fs.open(new Path(uri));
            LOGGER.info("Starts reading file from HDFS");
            IOUtils.copyBytes(in, out, config, false);
            LOGGER.info("Ends reading file from HDFS");
            return out;
        } catch (IOException e) {
            LOGGER.error("Can't load file for reading", e);
        } finally {
            IOUtils.closeStream(in);
        }
        return out;
    }

    /**
     * Method for writing files
     *
     * @param in <code>InputStream</code> with data
     */
    public static void writeFile(InputStream in) {
        try {
            config.set("fs.defaultFS", Config.loadProperty("hdfs.url"));
            config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            final String PATH_TO_FILE = Config.loadProperty("hdfs.file.avro");
            final String uri = HDFS_URL + PATH_TO_FILE;
            FileSystem fs = FileSystem.get(URI.create(uri), config);
            OutputStream out = fs.append(new Path(uri));
            LOGGER.info("Starts writing file to HDFS");
            IOUtils.copyBytes(in, out, config, true);
            LOGGER.info("Ends writing file to HDFS");
        } catch (IOException e) {
            LOGGER.error("Can't load file for writing", e);
        }
    }

}
