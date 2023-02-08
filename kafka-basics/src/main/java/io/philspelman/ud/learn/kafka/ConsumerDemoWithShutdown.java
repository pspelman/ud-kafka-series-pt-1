package io.philspelman.ud.learn.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    //    add a logger to the producer
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

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

        // before subscribing, create a shutdown hook
        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run(){
                log.info("Detected a shutdown, exiting with consumer.wakeup() ...");
                consumer.wakeup(); // this means an error will be thrown the next time the consumer wakes up

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // poll for data
            while (true) {
                log.info("Polling...");
                // set a timeout for time between polling so as not to overload Kafka broker
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // consume / handle any available data
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " , Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown...");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            log.info("Consumer is now gracefully shut down");
        }

    }

}
