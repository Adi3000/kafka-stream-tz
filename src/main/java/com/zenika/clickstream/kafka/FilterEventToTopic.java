package com.zenika.clickstream.kafka;

import com.zenika.clickstream.avro.CustomerSessionEvent;
import com.zenika.clickstream.kafka.config.KafkaConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

public class FilterEventToTopic implements Runnable, AutoCloseable {

    static final String INPUT_TOPIC = "CUSTOMER_SESSION";
    static final String OUTPUT_TOPIC = "CUSTOMER_SESSION_LOOKUP_PRODUCT";
    private static final String LOOKUP_PRODUCT_EVENT = "lookup_product";
    private static final String BOOTSTRAP_SERVERS_CONFIG = "clickstream.bootstrap.servers";
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "clickstream.schema.registry.url";
    private static final String APPLICATION_ID_CONFIG = "clickstream.application.id";

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<String, CustomerSessionEvent> consumer;
    private final KafkaProducer<String, CustomerSessionEvent> producer;

    public FilterEventToTopic() {

        var kafkaConfig = new KafkaConfig();
        this.consumer = new KafkaConsumer<>(kafkaConfig.getConsumerConfig());
        this.producer = new KafkaProducer<>(kafkaConfig.getProducerConfig());
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        try {
            while (running.get()) {
                consumeFromKafka();
            }
        } catch (WakeupException ignored) {
            if (running.get()) {
                throw ignored;
            }
        } finally {
            close();
        }
    }

    void consumeFromKafka(){
        ConsumerRecords<String, CustomerSessionEvent> records = consumer.poll(Duration.ofMillis(250));
        StreamSupport.stream(records.spliterator(), false)
                .map(ConsumerRecord::value)
                .filter(this::isLookupProduct)
                .forEach(this::sendToKafka);
        consumer.commitSync();
    }

    void sendToKafka(CustomerSessionEvent customerSessionEvent){
        try {
            producer.send(new ProducerRecord<>(OUTPUT_TOPIC, null, customerSessionEvent)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing filtered event", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to publish filtered event", e);
        }
    }

    public boolean isLookupProduct(CustomerSessionEvent event) {
        return Objects.nonNull(event)
                && event.getEventType() != null
                && LOOKUP_PRODUCT_EVENT.equalsIgnoreCase(event.getEventType().toString().trim());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (running.compareAndSet(true, false)) {
                consumer.wakeup();
            }
            producer.flush();
            producer.close(Duration.ofSeconds(10));
            consumer.close(Duration.ofSeconds(10));
        }
    }
}
