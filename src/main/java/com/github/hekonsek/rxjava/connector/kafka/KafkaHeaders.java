package com.github.hekonsek.rxjava.connector.kafka;

import com.github.hekonsek.rxjava.event.Event;

final public class KafkaHeaders {

    private KafkaHeaders() {
    }

    public static final String OFFSET = "rxjava.connector.kafka.offset";

    public static final String PARTITION = "rxjava.connector.kafka.partition";

    public static long offset(Event event) {
        return (long) event.headers().get(OFFSET);
    }

    public static int partition(Event event) {
        return (int) event.headers().get(PARTITION);
    }

}
