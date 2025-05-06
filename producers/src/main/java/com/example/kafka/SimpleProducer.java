package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {

        String topicName = "simple-topic"; // topic name
        // KafkaProducer configuration setting

        Properties props = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // Kafka 브로커의 주소 설정
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Key 직렬화 방법 설정(String)
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // value 직렬화 방법 설정(String)

        //KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        //ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName,null, "hello world");

        //kafkaProducer message send
        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
