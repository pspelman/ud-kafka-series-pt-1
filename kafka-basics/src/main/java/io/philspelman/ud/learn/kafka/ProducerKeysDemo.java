package io.philspelman.ud.learn.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKeysDemo {

    //    add a logger to the producer
    private static final Logger log = LoggerFactory.getLogger(ProducerKeysDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Hello World");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                // set the topic string
                String topic = "demo_java";
                // set the key string
                String key = "id_" + i;
                // set the value to be produced
                String value = "hello world " + i;


                // create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send (produce) data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // record was successfully sent
                            log.info("Received new metadata: " + "Key: " + key + " | Partition: " + metadata.partition());
                        } else {
                            // an error occurred while producing
                            log.error("Error while producing: ", e);
                        }
                    }
                });

            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // tell producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();

    }

}
