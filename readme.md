# RxJava Kafka connector

[![Version](https://img.shields.io/badge/RxJava%20Connector%20Kafka-0.3-blue.svg)](https://github.com/hekonsek/rxjava-connector-kafka/releases)
[![Build](https://api.travis-ci.org/hekonsek/rxjava-connector-kafka.svg)](https://travis-ci.org/hekonsek/rxjava-connector-kafka)
[![Coverage](https://sonarcloud.io/api/badges/measure?key=com.github.hekonsek%3Arxjava-connector-kafka&metric=coverage)](https://sonarcloud.io/component_measures?id=com.github.hekonsek%3Arxjava-connector-kafka&metric=coverage)

Connector between RxJava events and [Apache Kafka](https://kafka.apache.org) cluster.

## Installation

In order to start using Vert.x Pipes add the following dependency to your Maven project:

    <dependency>
      <groupId>com.github.hekonsek</groupId>
      <artifactId>vertx-connector-kafka</artifactId>
      <version>0.3</version>
    </dependency>

## Usage

This is how you can start consuming messages from Kafka topic:

```
import com.github.hekonsek.rxjava.connector.kafka.KafkaSource;

import static com.github.hekonsek.rxjava.connector.kafka.KafkaEventAdapter.simpleMapping;
import static com.github.hekonsek.rxjava.connector.kafka.KafkaHeaders.partition;
import static com.github.hekonsek.rxjava.connector.kafka.KafkaHeaders.offset;
import static com.github.hekonsek.rxjava.event.Headers.address;
import static com.github.hekonsek.rxjava.event.Headers.key;
...

new KafkaSource<String, String>(vertx(), topic).
  eventAdapter(simpleMapping(StringDeserializer.class, StringDeserializer.class)).build().
  subscribe(event -> {
    String payload = event.payload();
    String key = key(event);
    String topic = address(event);
    int partition = partition(event);
    int offset = partition(event);
  });
```

Default `eventAdapter` implementation assumes that key deserializer is `StringDeserializer` while value
deserializer is `BytesDeserializer` where value is encoded as JSON payload and converted to Map. So you can simply
consume JSON payload as follows: 

```
import com.github.hekonsek.rxjava.connector.kafka.KafkaSource;

import static com.github.hekonsek.rxjava.connector.kafka.KafkaEventAdapter.simpleMapping;
import static com.github.hekonsek.rxjava.connector.kafka.KafkaHeaders.partition;
import static com.github.hekonsek.rxjava.connector.kafka.KafkaHeaders.offset;
import static com.github.hekonsek.rxjava.event.Headers.address;
import static com.github.hekonsek.rxjava.event.Headers.key;
...

new KafkaSource<String, String>(vertx(), topic).build().
  subscribe(event -> {
    String key = key(event);
    Map<String, Object> payload = event.payload();
  });
```

## License

This project is distributed under Apache 2.0 license.