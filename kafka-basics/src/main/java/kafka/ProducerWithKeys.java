package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class ProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Starting Producer");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // infinitely produce messages
        while (true) {
            Random random = new Random();
            String key = "id_" + random.nextInt();
            ProducerRecord<String, String> record = new ProducerRecord<>("kafka_test", key, String.valueOf(random.nextInt()));
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    log.info("Successfully sent Record: {} \n Topic: {} \n Partition: {} \n" +
                                    "Offset: {} \n Timestamp {}", record, recordMetadata.topic(), recordMetadata.partition(),
                            recordMetadata.offset(), recordMetadata.timestamp());
                } else {
                    log.error("Error occured while sending", e);
                }
            });
            // demo to avoid sticky partitioning
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            producer.flush();
        }
    }
}
