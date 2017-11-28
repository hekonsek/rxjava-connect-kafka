package com.github.hekonsek.rxjava.pipes;

import com.github.hekonsek.rxjava.connect.kafka.KafkaSource;
import com.google.common.collect.ImmutableMap;
import io.debezium.kafka.KafkaCluster;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static com.github.hekonsek.rxjava.pipes.KafkaProducerBuilder.pipeProducer;
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
    public void shouldConsumeFromSource(TestContext context) {
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

}
