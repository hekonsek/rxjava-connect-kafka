/**
 * Licensed to the RxJava Connect Kafka under one or more
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
package com.github.hekonsek.rxjava.connect.kafka;

import com.github.hekonsek.rxjava.event.Event;
import com.google.common.collect.ImmutableMap;
import io.reactivex.Observable;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;

import java.util.Map;

import static com.github.hekonsek.rxjava.connect.kafka.KafkaEventAdapter.stringAndBytesToMap;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaSource<K, V> {

    private final Vertx vertx;

    private final String topic;

    private String bootstrapServers = "localhost:9092";

    private String groupId = randomUUID().toString();

    @SuppressWarnings("unchecked")
    private KafkaEventAdapter<K, V> eventAdapter = (KafkaEventAdapter<K, V>) stringAndBytesToMap();

    public KafkaSource(Vertx vertx, String topic) {
        this.vertx = vertx;
        this.topic = topic;
    }

    public Observable<Event<V>> build() {
        Map<String, String> config = ImmutableMap.of(
                GROUP_ID_CONFIG, groupId,
                BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                KEY_DESERIALIZER_CLASS_CONFIG, eventAdapter.getKeyDeserializer().getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, eventAdapter.getValueDeserializer().getName(),
                AUTO_OFFSET_RESET_CONFIG, "earliest");
        return KafkaConsumer.<K, V>create(vertx, config).subscribe(topic).
                toObservable().map(eventAdapter.getMapping());
    }

    public KafkaSource bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public KafkaSource groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public KafkaSource groupId(KafkaEventAdapter<K, V> eventAdapter) {
        this.eventAdapter = eventAdapter;
        return this;
    }

}