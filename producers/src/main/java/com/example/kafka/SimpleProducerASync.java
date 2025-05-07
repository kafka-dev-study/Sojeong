package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerASync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class.getName());
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
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName,null, "hello world2");

        //kafkaProducer message send
/*        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception == null) {
                    logger.info("\n ##### record metadata received ##### \n" +
                            "partition:" + metadata.partition() + "\n" +
                            "offset:" + metadata.offset() + "\n" +
                            "timestamp:" + metadata.timestamp());
                }else{
                    logger.error("exception error from broker" + exception.getMessage());
                }
            }
        });*/

        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("\n ##### record metadata received ##### \n" +
                        "partition:" + metadata.partition() + "\n" +
                        "offset:" + metadata.offset() + "\n" +
                        "timestamp:" + metadata.timestamp());
            } else{
                logger.error("exception error from broker " + exception.getMessage());
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        kafkaProducer.close();
    }
}
