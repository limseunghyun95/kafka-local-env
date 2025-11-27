package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        try {
            // set kafka properties
            Properties config = new Properties();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Broker
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // KEY Serializer
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Value Serializer

            // initialize kafka producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(config);

            // set Producer Record
            String topic = "sample";
            String key = "myKey";
            String value = "New record from Producer Sample";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // Send message
            // producer.send(record);

            // Sync send
            System.out.println("동기적으로 메시지 전송");
            producer.send(record).get();
        } catch (Exception e) {
            System.out.println(e);
        }

    }
}