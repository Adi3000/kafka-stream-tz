package com.zenika.clickstream;

import com.zenika.clickstream.kafkastream.EnrichSessionWithProductInfo;

public final class ClickStreamApplication {

    private static final String STREAM_THREAD_NAME = "CustomerSessionProductTagsKafkaStreams";

    private ClickStreamApplication() {
    }

    public static void main(String[] args) {
        EnrichSessionWithProductInfo topology = new EnrichSessionWithProductInfo();

        Runtime.getRuntime().addShutdownHook(new Thread(topology::close, STREAM_THREAD_NAME + "-shutdown"));

        Thread streamThread = new Thread(topology, STREAM_THREAD_NAME);
        streamThread.start();
    }
}
