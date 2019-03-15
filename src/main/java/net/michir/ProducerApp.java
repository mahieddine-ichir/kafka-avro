package net.michir;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.michir.data.Person;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class ProducerApp {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.103:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.3.103:8081");

        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        int i = 0;
        while (true) {
            sendSync(producer, i);
            Thread.sleep(1000);
            i++;
        }


    }

    private static void sendSync(Producer<String, GenericRecord> producer, Integer i) {
        try {

            Person person = Person.newBuilder()
                    .setLastname("Doe" + (i % 3))
                    .setFirtname("John "+i)
                    .setAge(20 + (i % 50))
                    .build();

            RecordMetadata recordMetadata = (RecordMetadata) producer
                    .send(new ProducerRecord("mic", ""+i, person)).get();

            System.out.println(String.format("%s %d %d - age = %d", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), person.getAge())
            );
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /*
    private static void sendASync(Producer<String, String> producer, Integer i) {
            producer.send(new ProducerRecord("avro", i, Integer.toString(i)), (recordMetadata, exception) -> {
                System.out.println(String.format("%d - %s %d %d",
                        i, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset())
                );
                if (exception != null) {
                    exception.printStackTrace();
                }
            });
    }*/

}
