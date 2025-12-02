package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException; // 파일 I/O 예외 처리를 위해 추가
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);
    private static final String OUTPUT_FILE_PATH = "./output.log";

    public static void main(String[] args) {
        // 1. Set Consumer Properties
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Broker
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Key Deserializer
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Value Deserializer
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "sample_group"); // Group ID 수정

        // 2. Initialize kafka consumer and file writer using try-with-resources
        // try-with-resources 구문으로 KafkaConsumer와 PrintWriter를 모두 안전하게 관리
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
             PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(OUTPUT_FILE_PATH, true)))) {

            // JVM 종료 시 안전하게 컨슈머를 종료하기 위한 셧다운 훅 등록
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Caught shutdown hook, waking up consumer...");
                consumer.wakeup(); // 컨슈머 폴링 루프 강제 종료
            }));

            // 3. Subscribe topic
            consumer.subscribe(Collections.singletonList("sample"));

            Duration timeout = Duration.ofMillis(100);

            // 4. Continuous Polling Loop
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(timeout);

                for (ConsumerRecord<String, String> record : records) {
                    // 기록할 문자열 포맷 정의 (줄바꿈 문자는 out.println()이 처리)
                    String recordLine = String.format("topic: %s, partition: %d, offset: %d, key: %s, value: %s",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());

                    System.out.println(recordLine);
                    // 파일에 한 줄씩 기록하고 즉시 flush
                    out.println(recordLine);
                    out.flush();
                }
            }
        } catch (WakeupException e) {
            // consumer.wakeup() 호출로 인한 정상적인 종료 (셧다운 훅)
            log.info("Consumer poll loop received WakeupException. Exiting gracefully.");
        } catch (IOException e) {
            // 파일 쓰기 중 발생한 I/O 오류 처리
            log.error("Error writing to file: {}", OUTPUT_FILE_PATH, e);
        } catch (Exception e) {
            // 기타 예상치 못한 예외 처리
            log.error("Unexpected error in consumer loop: ", e);
        } finally {
            // try-with-resources 덕분에 close()는 자동 호출됨
            log.info("Consumer and file resources are closed.");
        }
    }
}