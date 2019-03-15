package net.michir;

import net.michir.data.Person;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Map;

public class PersonSerdes {

  public static class PersonSerializer implements Serializer<Person> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Person data) {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);

        oos.writeObject(data);
        oos.close();

        return bos.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {

    }
  }

  public static class PersonDeserializer implements Deserializer<Person> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Person deserialize(String topic, byte[] data) {
      try {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
          return (Person) ois.readObject();
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {

    }
  }

}
