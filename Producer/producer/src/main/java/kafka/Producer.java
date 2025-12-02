package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class Producer {

    // 1. Callback 인터페이스 구현 클래스 (static 이너 클래스 권장)
    // static으로 선언하여 외부 Producer 인스턴스에 종속되지 않게 합니다.
    private static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            // 2. onCompletion 매개변수 오류 수정 (RecordMetadata 타입 중복 제거)
            if (e != null) {
                // 전송 실패 시 예외 처리
                System.err.println("메시지 전송 실패: " + e.getMessage());
                e.printStackTrace();
            } else {
                System.out.println("카프카 브로커에 메시지를 정상적으로 전송되었습니다.");
            }
        }
    }

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
            String value = "New record from Producer Sample 10";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // Send message
             producer.send(record);
             producer.flush();

            // Sync send
//            System.out.println("동기적으로 메시지 전송");
//            producer.send(record).get();

            // Async send
//            System.out.println("비동기적으로 메시지 전송");
//            producer.send(record, new DemoProducerCallback());
        } catch (Exception e) {
            System.out.println(e);
        }

    }
}