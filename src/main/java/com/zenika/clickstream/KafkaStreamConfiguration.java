package com.zenika.clickstream;

import com.zenika.clickstream.avro.CustomerSessionEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

@Getter
public class KafkaStreamConfiguration implements AutoCloseable{
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

    private final Serde<CustomerSessionEvent> customerSessionEventSerdes;
    public KafkaStreamConfiguration(){
        this.customerSessionEventSerdes = new SpecificAvroSerde<>();
    }

    public Properties buildStreamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, System.getProperty("clickstream.application.id", "clickstream"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("clickstream.bootstrap.servers", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        props.put(SCHEMA_REGISTRY_URL_CONFIG, System.getProperty("clickstream.schema.registry.url", "http://localhost:8081"));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.DefaultProductionExceptionHandler");
        return props;
    }

    @Override
    public void close() {
        customerSessionEventSerdes.close();
    }
}
