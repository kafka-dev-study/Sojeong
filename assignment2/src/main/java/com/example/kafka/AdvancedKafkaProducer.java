package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Scanner;

public class AdvancedKafkaProducer {

    public static void main(String[] args) throws Exception {
        String topicName = "user-log";

        // Kafka 설정
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);
        ObjectMapper mapper = new ObjectMapper();

        System.out.println("Kafka 사용자 로그 입력 (exit 입력 시 종료)");

        while (true) {
            System.out.print("사용자 ID: ");
            String userId = scanner.nextLine();
            if (userId.equalsIgnoreCase("exit")) break;

            System.out.print("활동 타입 (예: CLICK, VIEW, LOGIN 등): ");
            String activityType = scanner.nextLine();
            System.out.print("활동 설명: ");
            String description = scanner.nextLine();

            // JSON 객체 생성
            UserActivity activity = new UserActivity(userId, activityType, description);
            String jsonValue = mapper.writeValueAsString(activity);

            // 메시지 전송
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, userId, jsonValue);
            producer.send(record, (metadata, e) -> {
                if (e == null) {
                    System.out.println("▶ 전송 완료 - partition: " + metadata.partition() + ", offset: " + metadata.offset());
                } else {
                    e.printStackTrace();
                }
            });
        }

        producer.flush();
        producer.close();
    }

    // JSON 직렬화를 위한 POJO
    static class UserActivity {
        public String userId;
        public String activityType;
        public String description;

        public UserActivity() {} // Jackson용 기본 생성자
        public UserActivity(String userId, String activityType, String description) {
            this.userId = userId;
            this.activityType = activityType;
            this.description = description;
        }
    }
}