package com.github.hekonsek.rxjava.connect.kafka;

import com.github.hekonsek.rxjava.event.Event;
import io.reactivex.functions.Function;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;
import lombok.Data;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.Collections;
import java.util.Map;

import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.json.Json.decodeValue;

@Data
public class KafkaEventAdapter<K,V> {

    private final Class<? extends Deserializer> keyDeserializer;

    private final Class<? extends Deserializer> valueDeserializer;

    private final Function<KafkaConsumerRecord<K,V>, Event<V>> mapping;

    static KafkaEventAdapter<String, Map<String, Object>> stringAndBytesToMap() {
        return new KafkaEventAdapter<>(StringDeserializer.class, BytesDeserializer.class,
                record -> new Event(Collections.emptyMap(), decodeValue(buffer(((Bytes) record.value()).get()), Map.class)));
    }

}
