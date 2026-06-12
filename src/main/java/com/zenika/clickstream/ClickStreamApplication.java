package com.zenika.clickstream;

import com.zenika.clickstream.kafkastream.FilterEventToTopic;

public final class ClickStreamApplication {

    private static final String STREAM_THREAD_NAME = "FilterEventKafkaStreams";

    private ClickStreamApplication() {
    }

    public static void main(String[] args) {
        FilterEventToTopic filterEventToTopic = new FilterEventToTopic();

        Runtime.getRuntime().addShutdownHook(new Thread(filterEventToTopic::close, STREAM_THREAD_NAME + "-shutdown"));

        Thread streamThread = new Thread(filterEventToTopic, STREAM_THREAD_NAME);
        streamThread.start();
    }
}
