package io.philspelman.ud.learn.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    //    add a logger to the producer
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("This is a Kafka Consumer...");

        // set the groupId variable
        String groupId = "my-java-application";

        // set the topic
        String topic = "demo_java";

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");


        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // set the groupId
        properties.setProperty("group.id", groupId);

        // set the offset from which to consume
        properties.setProperty("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // begin to poll for data
        while (true) {
            log.info("Polling...");
            // set a timeout for time between polling so as not to overload Kafka broker
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            // consume / handle any available data
            for (ConsumerRecord<String, String> record: records) {
                log.info("Key: " + record.key() + " , Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }

    }

}
