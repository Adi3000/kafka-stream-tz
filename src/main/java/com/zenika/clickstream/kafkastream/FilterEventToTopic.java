package com.zenika.clickstream.kafkastream;

import com.zenika.clickstream.KafkaStreamConfiguration;
import com.zenika.clickstream.avro.CustomerSessionEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

public class FilterEventToTopic implements Runnable , AutoCloseable {

    static final String INPUT_TOPIC = "CUSTOMER_SESSION";
    static final String OUTPUT_TOPIC = "CUSTOMER_SESSION_LOOKUP_PRODUCT";
    private static final String LOOKUP_PRODUCT_EVENT = "lookup_product";


    private final KafkaStreamConfiguration kafkaStreamConfiguration;
    private KafkaStreams streams;

    public FilterEventToTopic(){
        this.kafkaStreamConfiguration = new KafkaStreamConfiguration();
    }

    @Override
    public void run() {
        streams = new KafkaStreams(buildTopology(), kafkaStreamConfiguration.buildStreamsProperties());

        configureGracefulStop();
        streams.start();
    }

    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<CustomerSessionEvent> serdes = kafkaStreamConfiguration.getCustomerSessionEventSerdes();

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), serdes))
                .filter((_, event) -> isLookupProduct(event))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), serdes));

        return builder.build();
    }

    private boolean isLookupProduct(CustomerSessionEvent event) {
        return  Objects.nonNull(event) &&
                event.getEventType() != null &&
                LOOKUP_PRODUCT_EVENT.equalsIgnoreCase(event.getEventType().toString().trim());
    }

    @Override
    public void close() {
        kafkaStreamConfiguration.close();
        Optional.ofNullable(streams)
                .ifPresent(KafkaStreams::close);
    }


    private void configureGracefulStop() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(10)) ));
    }
}
