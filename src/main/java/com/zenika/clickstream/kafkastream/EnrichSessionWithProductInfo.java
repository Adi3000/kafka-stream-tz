package com.zenika.clickstream.kafkastream;

import com.zenika.clickstream.avro.CustomerSessionEvent;
import com.zenika.clickstream.avro.CustomerSessionProductTagsEvent;
import com.zenika.clickstream.avro.ProductInformationEvent;
import com.zenika.clickstream.kafkastream.config.KafkaStreamConfiguration;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KeyValue;

public class EnrichSessionWithProductInfo implements Runnable, AutoCloseable {

    static final String CUSTOMER_SESSION_TOPIC = "CUSTOMER_SESSION";
    static final String PRODUCT_INFORMATION_TOPIC = "PRODUCT_INFORMATION";
    static final String OUTPUT_TOPIC = "CUSTOMER_SESSION_PRODUCT_TAGS";

    private static final String LOOKUP_PRODUCT_EVENT = "lookup_product";
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final Pattern JSON_FIELD_PATTERN = Pattern.compile("\"%s\"\\s*:\\s*\"?([^\",}]+)\"?");
    private static final Pattern KVP_FIELD_PATTERN = Pattern.compile("(?i)(?:^|[&;,\\s])%s\\s*[:=]\\s*([^&;,\\s]+)");

    private final KafkaStreamConfiguration kafkaStreamConfiguration;
    private final Serde<CustomerSessionEvent> customerSessionSerde;
    private final Serde<ProductInformationEvent> productInformationSerde;
    private final Serde<CustomerSessionProductTagsEvent> customerSessionProductTagsSerde;
    private KafkaStreams streams;

    public EnrichSessionWithProductInfo() {
        this.kafkaStreamConfiguration = new KafkaStreamConfiguration();
        this.customerSessionSerde = configuredValueSerde(new SpecificAvroSerde<>());
        this.productInformationSerde = configuredValueSerde(new SpecificAvroSerde<>());
        this.customerSessionProductTagsSerde = configuredValueSerde(new SpecificAvroSerde<>());
    }

    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(CUSTOMER_SESSION_TOPIC, Consumed.with(Serdes.String(), customerSessionSerde))
                .filter((_, event) -> isLookupProduct(event))
                .flatMap((_, event) -> extractProductId(event)
                        .map(productId -> List.of(KeyValue.pair(productId, event)))
                        .orElseGet(Collections::emptyList))
                .join(builder.table(PRODUCT_INFORMATION_TOPIC, Consumed.with(Serdes.String(), productInformationSerde)),
                        this::enrichSessionWithTags,
                        Joined.with(Serdes.String(), customerSessionSerde, productInformationSerde))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), customerSessionProductTagsSerde));

        return builder.build();
    }

    @Override
    public void run() {
        streams = new KafkaStreams(buildTopology(), kafkaStreamConfiguration.buildStreamsProperties());
        configureGracefulStop();
        streams.start();
    }

    private CustomerSessionProductTagsEvent enrichSessionWithTags(CustomerSessionEvent sessionEvent,
                                                                  ProductInformationEvent productInformationEvent) {
        // The current customer session schema does not carry session_id directly, so derive it from metadata first.
        String sessionId = extractSessionId(sessionEvent).orElseGet(() -> Objects.toString(sessionEvent.getCustomerId(), ""));
        String timestamp = Objects.toString(sessionEvent.getTimestamp(), "");
        String productId = extractProductId(sessionEvent)
                .orElseGet(() -> Objects.toString(productInformationEvent.getProductId(), ""));
        List<String> tags = Optional.ofNullable(productInformationEvent.getTags()).orElseGet(Collections::emptyList);

        return CustomerSessionProductTagsEvent.newBuilder()
                .setSessionId(sessionId)
                .setTimestamp(timestamp)
                .setProductId(productId)
                .setTags(tags)
                .build();
    }

    private boolean isLookupProduct(CustomerSessionEvent event) {
        return Objects.nonNull(event)
                && event.getEventType() != null
                && LOOKUP_PRODUCT_EVENT.equalsIgnoreCase(event.getEventType().toString().trim());
    }

    private Optional<String> extractProductId(CustomerSessionEvent event) {
        return extractMetadataValue(event, "product_id");
    }

    private Optional<String> extractSessionId(CustomerSessionEvent event) {
        return extractMetadataValue(event, "session_id");
    }

    private Optional<String> extractMetadataValue(CustomerSessionEvent event, String fieldName) {
        if (event == null || event.getMetadata() == null) {
            return Optional.empty();
        }

        String metadata = event.getMetadata().toString().trim();
        if (metadata.isEmpty()) {
            return Optional.empty();
        }

        Optional<String> jsonValue = extractWithPattern(JSON_FIELD_PATTERN, metadata, fieldName);
        if (jsonValue.isPresent()) {
            return jsonValue;
        }

        Optional<String> keyValue = extractWithPattern(KVP_FIELD_PATTERN, metadata, fieldName);
        if (keyValue.isPresent()) {
            return keyValue;
        }

        if (!metadata.contains("=") && !metadata.contains(":")) {
            return Optional.of(metadata);
        }

        return Optional.empty();
    }

    private Optional<String> extractWithPattern(Pattern template, String metadata, String fieldName) {
        Matcher matcher = Pattern.compile(String.format(template.pattern(), Pattern.quote(fieldName))).matcher(metadata);
        if (matcher.find()) {
            String value = matcher.group(1);
            if (value != null && !value.isBlank()) {
                return Optional.of(value.trim());
            }
        }
        return Optional.empty();
    }

    private <T extends SpecificRecord> Serde<T> configuredValueSerde(SpecificAvroSerde<T> serde) {
        serde.configure(Map.of(
                SCHEMA_REGISTRY_URL_CONFIG,
                System.getProperty("clickstream.schema.registry.url", "http://localhost:8081")
        ), false);
        return serde;
    }

    @Override
    public void close() {
        kafkaStreamConfiguration.close();
        Optional.ofNullable(streams)
                .ifPresent(kafkaStreams -> kafkaStreams.close(Duration.ofSeconds(10)));
    }

    private void configureGracefulStop() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(10))));
    }
}
