package com.github.calmera.eda.todo;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.lang.NonNull;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${app.topics.todo.events}")
    private String todoEventTopic;

    private final Deserializer<Object> avroDeserializer = new KafkaAvroDeserializer();
    private final Serializer<Object> avroSerializer = new KafkaAvroSerializer();

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "streams-app");
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(AbstractKafkaAvroSerDeConfig.USE_LATEST_VERSION, true);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    KafkaStreamsInfrastructureCustomizer messageTopologyCustomizer() {
        return new KafkaStreamsInfrastructureCustomizer() {
            @Override
            public void configureTopology(@NonNull Topology topology) {
                topology.addSource("input", Serdes.String().deserializer(), avroDeserializer, todoEventTopic)
                        .addProcessor("dispatcher", TodoEventDispatcher::new, "input")
                        .addSink("output", todoEventTopic, Serdes.String().serializer(), avroSerializer, "dispatcher");
            }
        };
    }

    @Bean
    StreamsBuilderFactoryBeanCustomizer streamsBuilderFactoryBeanCustomizer(
            KafkaStreamsInfrastructureCustomizer customizer) {
        return (streamsBuilderFactoryBean) -> {
            streamsBuilderFactoryBean.setInfrastructureCustomizer(customizer);
        };
    }
}
