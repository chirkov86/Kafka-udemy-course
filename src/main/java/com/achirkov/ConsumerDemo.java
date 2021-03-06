package com.achirkov;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        /*
        Assign and seek is another kind of API that does not use group and fits more util needs
        like replaying data.
         */

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Group is not used in this API
        // properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_app");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // we want to read from the beginning of the topic

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        final TopicPartition topicPartition = new TopicPartition("first_topic", 0);
        consumer.assign(List.of(topicPartition));

        // Seeking messages from offset 3
        consumer.seek(topicPartition, 3);

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("key {} value {} partition {} offset {}",
                        record.key(), record.value(), record.partition(), record.offset());
            }
        }
    }
}
