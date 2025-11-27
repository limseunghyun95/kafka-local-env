package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Sample {

    public static void main(String[] args) {


        // 1. Kafka 설정 정의 (Properties 객체)
        Properties kafkaProps = new Properties();
        // 브로커 목록 지정 (최소 하나 이상 필요)
        kafkaProps.put("bootstrap.servers", "localhost:9092");

        // 메시지의 키와 값을 직렬화할 클래스 지정
        // Kafka는 네트워크로 데이터를 보내기 전에 데이터를 바이트 배열로 변환(직렬화)해야 합니다.
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2. KafkaProducer 객체 생성 및 메시지 전송 (try-with-resources 사용)
        // try-with-resources 구문은 프로듀서를 사용 후 자동으로 .close()를 호출하여 안전하게 종료합니다.
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {

            // 3. 전송할 메시지 생성 (ProducerRecord 객체)
            // 토픽 이름, 키(선택 사항), 값(메시지 내용)을 지정합니다.
            String topic = "sample"; // 메시지를 보낼 Kafka 토픽 이름
            String key = "myKey";        // 메시지의 키 (선택 사항, 지정하면 같은 키는 같은 파티션에 저장됨)
            String value = "Hello Kafka from Java!"; // 실제 메시지 내용

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // 4. 메시지 전송
            producer.send(record);
            System.out.println("✅ Message sent successfully to topic: " + topic);

            // send()는 비동기로 동작합니다. 버퍼에 쌓인 데이터를 즉시 보내도록 강제합니다.
            // 작은 프로그램에서는 생략해도 되지만, 보통 메시지가 손실되지 않도록 flush()를 호출하는 것이 좋습니다.
            producer.flush();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("❌ Error sending message to Kafka: " + e.getMessage());
        }
    }
}