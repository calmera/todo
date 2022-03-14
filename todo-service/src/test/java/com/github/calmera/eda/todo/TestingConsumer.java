package com.github.calmera.eda.todo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestingConsumer<V> implements Closeable {
    private final String topic;
    private final EmbeddedKafkaBroker broker;
    private final ConsumerFactory<String, V> consumerFactory;

    private BlockingQueue<ConsumerRecord<String, V>> records;
    private KafkaMessageListenerContainer<String, V> container;

    public TestingConsumer(EmbeddedKafkaBroker broker, String topic, Deserializer<V> valueDeserializer) {
        this.broker = broker;
        this.topic = topic;

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("consumer", "false", broker));
        this.consumerFactory = new DefaultKafkaConsumerFactory<>(configs, Serdes.String().deserializer(), valueDeserializer);
    }

    public void setup() {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, V>) records::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());
    }

    public ConsumerRecord<String, V> poll(long millis) throws InterruptedException {
        return records.poll(millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        container.stop();
    }
}
