package com.example.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AdvancedKafkaConsumer {
    public static void main(String[] args) throws Exception {
        String topicName = "user-log";

        // Consumer 설정
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "log-consumer-group");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 수동 커밋
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 처음부터

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("Kafka Consumer 시작...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode node = mapper.readTree(record.value());
                    String activityType = node.get("activityType").asText();

                    if ("CLICK".equalsIgnoreCase(activityType)) {
                        System.out.println("▶ CLICK 메시지 수신");
                        System.out.println("  Key: " + record.key());
                        System.out.println("  전체 JSON: " + record.value());
                        System.out.println("  Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                } catch (Exception e) {
                    System.err.println("JSON 파싱 오류: " + e.getMessage());
                }
            }

            // 수동 커밋
            consumer.commitSync();
        }
    }
}
