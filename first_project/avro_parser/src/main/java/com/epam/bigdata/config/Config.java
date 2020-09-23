package com.epam.bigdata.config;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

public class Config {

    private static final Logger LOGGER = LogManager.getLogger();
    private static Configuration config;

    static {
        Configurations configs = new Configurations();
        try {
            config = configs.properties(new File("avro_parser.properties"));
        } catch (ConfigurationException cex) {
            LOGGER.error("Can't load the properties", cex);
        }
    }

    public static String loadProperty(String name) {
        return config.getString(name);
    }

}
