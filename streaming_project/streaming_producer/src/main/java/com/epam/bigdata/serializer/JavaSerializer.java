package com.epam.bigdata.serializer;

import com.epam.bigdata.model.Hotel;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class JavaSerializer implements Serializer<Hotel> {

    @Override
    public byte[] serialize(String s, Hotel o) {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
            objectStream.writeObject(o);
            objectStream.flush();
            objectStream.close();
            return byteStream.toByteArray();
        }
        catch (IOException e) {
            throw new IllegalStateException("Can't serialize object: " + o, e);
        }
    }

}
