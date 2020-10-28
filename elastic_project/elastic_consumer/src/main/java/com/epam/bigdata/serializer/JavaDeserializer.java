package com.epam.bigdata.serializer;

import com.epam.bigdata.model.Hotel;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.*;

public class JavaDeserializer implements Deserializer<Hotel> {

    @Override
    public Hotel deserialize(String s, byte[] bytes) {
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
            return (Hotel) objectStream.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Can't serialize object", e);
        }
    }
}
