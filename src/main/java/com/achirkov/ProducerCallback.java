package com.achirkov;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallback {
    public static void main(String[] args) {

        final Logger log = LoggerFactory.getLogger(ProducerCallback.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world");

        // send data asynchronously !!!
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                log.info("Received new metadata: \n" +
                        "Topic: {} \n" +
                        "Partition {} \n" +
                        "Offset {}", metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                log.error("Error occurred", exception);
            }
        });

        producer.flush();
        producer.close();
    }
}
