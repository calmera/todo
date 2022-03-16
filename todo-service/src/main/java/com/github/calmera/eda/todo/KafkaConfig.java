package com.github.calmera.eda.todo;

import io.confluent.kafka.serializers.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.lang.NonNull;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Value(value = "${confluent_broker_endpoint}")
    private String bootstrapAddress;

    @Value(value = "${app.topics.todo.events}")
    private String todoEventTopic;

//    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
//    KafkaStreamsConfiguration kStreamsConfig(Environment env) {
//        Map<String, Object> props = new HashMap<>();
//        props.put(APPLICATION_ID_CONFIG, "streams-app");
//        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
//        props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
//        props.put(AbstractKafkaAvroSerDeConfig.USE_LATEST_VERSION, true);
//        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, env.getProperty("spring.kafka.properties.schema.registry.url"));
//
//        return new KafkaStreamsConfiguration(props);
//    }

    @Bean
    KafkaStreamsInfrastructureCustomizer messageTopologyCustomizer(KafkaAvroDeserializer avroDeserializer, KafkaAvroSerializer avroSerializer) {
        return new KafkaStreamsInfrastructureCustomizer() {
            @Override
            public void configureTopology(@NonNull Topology topology) {
                topology.addSource("input", Serdes.String().deserializer(), avroDeserializer, todoEventTopic)
                        .addProcessor("dispatcher", TodoEventDispatcher::new, "input")
                        .addSink("output", todoEventTopic, Serdes.String().serializer(), avroSerializer, "dispatcher")
                        .addStateStore(new KeyValueStoreBuilder<>(
                                Stores.persistentKeyValueStore("todos"), Serdes.String(), Serdes.serdeFrom(avroSerializer, avroDeserializer), Time.SYSTEM
                        ), "dispatcher");
            }
        };
    }

    @Bean
    StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanCustomizer(
            KafkaStreamsInfrastructureCustomizer customizer) {
        return (streamsBuilderFactoryBean) -> {
            streamsBuilderFactoryBean.setInfrastructureCustomizer(customizer);
        };
    }

    @ConditionalOnMissingBean
    @Bean
    KafkaAvroSerializer avroSerializer(KafkaProperties properties) {
        Map<String, Object> props = properties.buildProducerProperties();

        return new KafkaAvroSerializer(null, props);
    }

    @ConditionalOnMissingBean
    @Bean
    KafkaAvroDeserializer avroDeserializer(KafkaProperties properties) {
        Map<String, Object> props = properties.buildConsumerProperties();
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        return new KafkaAvroDeserializer(null, props);
    }
}
