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
import io.reactivex.functions.Function;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import lombok.Data;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.Map;

import static com.github.hekonsek.rxjava.event.Headers.ADDRESS;
import static com.github.hekonsek.rxjava.event.Headers.KEY;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.json.Json.decodeValue;

@Data
public class KafkaEventAdapter<K, V> {

    private final Class<? extends Deserializer> keyDeserializer;

    private final Class<? extends Deserializer> valueDeserializer;

    private final Function<KafkaConsumerRecord<K, V>, Event<V>> mapping;

    static KafkaEventAdapter<String, Map<String, Object>> stringAndBytesToMap() {
        return new KafkaEventAdapter<>(StringDeserializer.class, BytesDeserializer.class,
                record -> {
                    Map<String, Object> headers = ImmutableMap.of(
                            KEY, record.key(),
                            ADDRESS, record.topic()
                    );
                    Map<String, Object> value = decodeValue(buffer(((Bytes) record.value()).get()), Map.class);
                    return new Event<>(headers, value);
                });
    }

}