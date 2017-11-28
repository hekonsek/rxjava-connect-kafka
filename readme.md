# RxJava Kafka connector

[![Version](https://img.shields.io/badge/RxJava%20Connect%20Kafka-0.1-blue.svg)](https://github.com/hekonsek/rxjava-connect-kafka/releases)
[![Build](https://api.travis-ci.org/hekonsek/rxjava-connect-kafka.svg)](https://travis-ci.org/hekonsek/rxjava-connect-kafka)

Connector between RxJava events and [Apache Kafka](https://kafka.apache.org) cluster.

## Installation

In order to start using Vert.x Pipes add the following dependency to your Maven project:

    <dependency>
      <groupId>com.github.hekonsek</groupId>
      <artifactId>vertx-connect-kafka</artifactId>
      <version>0.1</version>
    </dependency>

## Usage



```
import static com.github.hekonsek.rxjava.connect.kafka.KafkaEventAdapter.simpleMapping;
import static com.github.hekonsek.rxjava.event.Headers.address;
import static com.github.hekonsek.rxjava.event.Headers.key;
...

new KafkaSource<String, String>(vertx(), topic).
  eventAdapter(simpleMapping(StringDeserializer.class, StringDeserializer.class)).build().
  subscribe(event -> {
    String payload = event.payload();
    String key = key(event);
    String topic = address(event);
  });
```

## License

This project is distributed under Apache 2.0 license.