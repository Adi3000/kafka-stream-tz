package com.zenika.clickstream.kafkastream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zenika.clickstream.avro.CustomerSessionEvent;
import com.zenika.clickstream.kafka.FilterEventToTopic;
import org.junit.jupiter.api.Test;

class FilterEventToTopicConsumerProducerTest {

    @Test
    void identifiesLookupProductEvents() {
        assertTrue(new FilterEventToTopic().isLookupProduct(event("lookup_product")));
        assertTrue(new FilterEventToTopic().isLookupProduct(event(" LOOKUP_PRODUCT ")));
        assertFalse(new FilterEventToTopic().isLookupProduct(event("page_view")));
        assertFalse(new FilterEventToTopic().isLookupProduct(null));
    }

    private static CustomerSessionEvent event(String eventType) {
        return CustomerSessionEvent.newBuilder()
                .setCustomerId("c-1")
                .setEventType(eventType)
                .setTimestamp("2026-06-12T10:00:00Z")
                .setMetadata("sku=123")
                .build();
    }
}
