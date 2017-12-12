/**
 * Licensed to the RxJava Connector Kafka under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.hekonsek.rxjava.connector.kafka;

import com.github.hekonsek.rxjava.connector.kafka.KafkaSource;
import com.google.common.collect.ImmutableMap;
import io.debezium.kafka.KafkaCluster;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static com.github.hekonsek.rxjava.connector.kafka.KafkaEventAdapter.simpleMapping;
import static com.github.hekonsek.rxjava.connector.kafka.KafkaHeaders.offset;
import static com.github.hekonsek.rxjava.connector.kafka.KafkaHeaders.partition;
import static com.github.hekonsek.rxjava.connector.kafka.KafkaProducerBuilder.pipeProducer;
import static com.github.hekonsek.rxjava.event.Headers.ADDRESS;
import static com.github.hekonsek.rxjava.event.Headers.KEY;
import static com.google.common.io.Files.createTempDir;
import static io.vertx.reactivex.core.Vertx.vertx;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(VertxUnitRunner.class)
public class KafkaSourceTest {

    @BeforeClass
    public static void beforeClass() throws IOException {
        new KafkaCluster().addBrokers(1).withPorts(2181, 9092).
                usingDirectory(createTempDir()).deleteDataPriorToStartup(true).
                startup();
    }

    String topic = randomUUID().toString();

    @Test
    public void shouldConsumePayload(TestContext context) {
        Async async = context.async();
        new KafkaSource<String, Map>(vertx(), topic).build().
                map(event ->  (String) event.payload().get("foo")).
                subscribe(event -> {
                    assertThat(event).isEqualTo("bar");
                    async.complete();
                });

        Bytes event = new Bytes(Json.encode(ImmutableMap.of("foo", "bar")).getBytes());
        pipeProducer(vertx()).rxWrite(KafkaProducerRecord.create(topic, "key", event)).subscribe();
    }

    @Test
    public void shouldConsumeKafkaMetadata(TestContext context) {
        Async async = context.async();
        new KafkaSource<String, Map>(vertx(), topic).build().
                subscribe(event -> {
                    assertThat(event.headers()).containsEntry(KEY, "key").containsEntry(ADDRESS, topic);
                    assertThat(offset(event)).isGreaterThanOrEqualTo(0);
                    assertThat(partition(event)).isEqualTo(0);
                    async.complete();
                });

        Bytes event = new Bytes(Json.encode(ImmutableMap.of("foo", "bar")).getBytes());
        pipeProducer(vertx()).rxWrite(KafkaProducerRecord.create(topic, "key", event)).subscribe();
    }

    @Test
    public void shouldConsumeString(TestContext context) {
        Async async = context.async();
        new KafkaSource<String, String>(vertx(), topic).
                eventAdapter(simpleMapping(StringDeserializer.class, StringDeserializer.class)).build().
                subscribe(event -> {
                    assertThat(event.payload()).isEqualTo("foo");
                    async.complete();
                });

        Map<String, String> config = ImmutableMap.of(
                "bootstrap.servers", "localhost:9092",
                "value.serializer", StringSerializer.class.getName(),
                "key.serializer", StringSerializer.class.getName());
        KafkaProducer.create(vertx(), config).rxWrite(KafkaProducerRecord.create(topic, "key", "foo")).subscribe();
    }

}
