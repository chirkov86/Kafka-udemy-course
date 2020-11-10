package com.achirkov;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoThreads.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_app");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // we want to read from the beginning of the topic

        final CountDownLatch latch = new CountDownLatch(1);
        ConsumerTask consumerTask = new ConsumerTask(properties, latch);
        new Thread(consumerTask).start();

        // Adding a shutdown hook to gracefully shutdown Kafka consumer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown signal");
            consumerTask.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                logger.info("Shutdown hook has finished");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application interrupted");
        } finally {
            logger.info("Application closing");
        }
    }

    static class ConsumerTask implements Runnable {

        private static final Logger logger = LoggerFactory.getLogger(ConsumerTask.class);

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;

        public ConsumerTask(final Properties properties, final CountDownLatch latch) {
            this.latch = latch;
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(List.of("first_topic"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key {} value {} partition {} offset {}",
                                record.key(), record.value(), record.partition(), record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("received shutdown signal");
            } finally {
                logger.info("Shutting down Kafka consumer");
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            // interrupting poll
            consumer.wakeup();
        }
    }
}
