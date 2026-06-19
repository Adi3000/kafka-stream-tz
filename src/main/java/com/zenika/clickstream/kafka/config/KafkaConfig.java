package com.zenika.clickstream.kafka.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

@Getter
public class KafkaConfig {

    private Properties consumerConfig;
    private Properties producerConfig;

    public KafkaConfig(){
        initKafkaConfig();
    }

    private void initKafkaConfig(){
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
        consumerConfig.put(GROUP_ID_CONFIG,
                System.getProperty(GROUP_ID_CONFIG, "clickstream"));
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerConfig.put("schema.registry.url", System.getProperty(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"));
        consumerConfig.put("specific.avro.reader", "true");

        producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
        producerConfig.put("schema.registry.url", System.getProperty(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"));
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        producerConfig.put("acks", "all");
    }
}
