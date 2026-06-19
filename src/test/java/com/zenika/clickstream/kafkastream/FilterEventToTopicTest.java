package com.zenika.clickstream.kafkastream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zenika.clickstream.kafkastream.config.KafkaStreamConfiguration;
import com.zenika.clickstream.avro.CustomerSessionEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;

class FilterEventToTopicTest {

    @Test
    void filtersLookupProductEvents() {
        System.setProperty("clickstream.schema.registry.url", "mock://clickstream");

        KafkaStreamConfiguration configuration = new KafkaStreamConfiguration();
        FilterEventToTopic filterEventToTopic = new FilterEventToTopic();

        try (TopologyTestDriver testDriver = new TopologyTestDriver(
                filterEventToTopic.buildTopology(),
                configuration.buildStreamsProperties())) {
            final SpecificAvroSerde<CustomerSessionEvent> serde = new SpecificAvroSerde<>();
            serde.configure(Map.of("schema.registry.url", "mock://clickstream"), false);

            TestInputTopic<String, CustomerSessionEvent> inputTopic = testDriver.createInputTopic(
                    FilterEventToTopic.INPUT_TOPIC,
                    Serdes.String().serializer(),
                    serde.serializer());
            TestOutputTopic<String, CustomerSessionEvent> outputTopic = testDriver.createOutputTopic(
                    FilterEventToTopic.OUTPUT_TOPIC,
                    Serdes.String().deserializer(),
                    serde.deserializer());

            inputTopic.pipeInput("1", event("c-1", "lookup_product", "2026-06-12T10:00:00Z", "sku=123"));
            inputTopic.pipeInput("2", event("c-2", "page_view", "2026-06-12T10:01:00Z", "home"));

            KeyValue<String, CustomerSessionEvent> result = outputTopic.readKeyValue();
            assertEquals("1", result.key);
            assertEquals("c-1", result.value.getCustomerId().toString());
            assertEquals("lookup_product", result.value.getEventType().toString());
            assertTrue(outputTopic.isEmpty());
        }
    }

    private static CustomerSessionEvent event(String customerId, String eventType, String timestamp, String metadata) {
        return CustomerSessionEvent.newBuilder()
                .setCustomerId(customerId)
                .setEventType(eventType)
                .setTimestamp(timestamp)
                .setMetadata(metadata)
                .build();
    }
}
