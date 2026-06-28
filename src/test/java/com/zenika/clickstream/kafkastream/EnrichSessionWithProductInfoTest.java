package com.zenika.clickstream.kafkastream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zenika.clickstream.avro.CustomerSessionEvent;
import com.zenika.clickstream.avro.CustomerSessionProductTagsEvent;
import com.zenika.clickstream.avro.ProductInformationEvent;
import com.zenika.clickstream.kafkastream.config.KafkaStreamConfiguration;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;

class EnrichSessionWithProductInfoTest {

    @Test
    void enrichesLookupProductSessionsWithProductTags() {
        System.setProperty("clickstream.schema.registry.url", "mock://clickstream");

        KafkaStreamConfiguration configuration = new KafkaStreamConfiguration();
        EnrichSessionWithProductInfo topology = new EnrichSessionWithProductInfo();

        try (TopologyTestDriver testDriver = new TopologyTestDriver(
                topology.buildTopology(),
                configuration.buildStreamsProperties())) {
            SpecificAvroSerde<CustomerSessionEvent> sessionSerde = new SpecificAvroSerde<>();
            sessionSerde.configure(Map.of("schema.registry.url", "mock://clickstream"), false);

            SpecificAvroSerde<ProductInformationEvent> productSerde = new SpecificAvroSerde<>();
            productSerde.configure(Map.of("schema.registry.url", "mock://clickstream"), false);

            SpecificAvroSerde<CustomerSessionProductTagsEvent> outputSerde = new SpecificAvroSerde<>();
            outputSerde.configure(Map.of("schema.registry.url", "mock://clickstream"), false);

            TestInputTopic<String, CustomerSessionEvent> customerSessionInput = testDriver.createInputTopic(
                    EnrichSessionWithProductInfo.CUSTOMER_SESSION_TOPIC,
                    Serdes.String().serializer(),
                    sessionSerde.serializer());
            TestInputTopic<String, ProductInformationEvent> productInformationInput = testDriver.createInputTopic(
                    EnrichSessionWithProductInfo.PRODUCT_INFORMATION_TOPIC,
                    Serdes.String().serializer(),
                    productSerde.serializer());
            TestOutputTopic<String, CustomerSessionProductTagsEvent> outputTopic = testDriver.createOutputTopic(
                    EnrichSessionWithProductInfo.OUTPUT_TOPIC,
                    Serdes.String().deserializer(),
                    outputSerde.deserializer());

            productInformationInput.pipeInput("123", productInformation("123", List.of("tag-a", "tag-b")));
            customerSessionInput.pipeInput("c-1", customerSession(
                    "c-1",
                    "lookup_product",
                    "2026-06-12T10:00:00Z",
                    "session_id=s-1;product_id=123"));

            KeyValue<String, CustomerSessionProductTagsEvent> result = outputTopic.readKeyValue();
            assertEquals("123", result.key);
            assertEquals("s-1", result.value.getSessionId().toString());
            assertEquals("2026-06-12T10:00:00Z", result.value.getTimestamp().toString());
            assertEquals("123", result.value.getProductId().toString());
            assertEquals(List.of("tag-a", "tag-b"), result.value.getTags());
            assertTrue(outputTopic.isEmpty());
        }
    }

    private static CustomerSessionEvent customerSession(String customerId,
                                                        String eventType,
                                                        String timestamp,
                                                        String metadata) {
        return CustomerSessionEvent.newBuilder()
                .setCustomerId(customerId)
                .setEventType(eventType)
                .setTimestamp(timestamp)
                .setMetadata(metadata)
                .build();
    }

    private static ProductInformationEvent productInformation(String productId, List<String> tags) {
        return ProductInformationEvent.newBuilder()
                .setProductId(productId)
                .setTags(tags)
                .build();
    }
}
