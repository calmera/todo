package com.github.calmera.eda.todo;

import com.github.calmera.eda.todo.logic.TodoReader;
import com.github.calmera.eda.todo.logic.kafka.GlobalKafkaTodoReader;
import com.github.calmera.eda.todo.logic.kafka.LocalKafkaTodoReader;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG;

//@EnableWebMvc
@Configuration
public class ApiConfig {
    @Bean
    public TodoReader localTodoReader(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        return new LocalKafkaTodoReader(streamsBuilderFactoryBean);
    }

    @Bean
    public TodoReader globalTodoReader(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        return new GlobalKafkaTodoReader(streamsBuilderFactoryBean);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(KafkaStreamsConfiguration kafkaStreamsConfiguration,
                                                       KafkaAvroSerializer avroSerializer) {

        Map<String, Object> producerProps = new HashMap<>();
        kafkaStreamsConfiguration.asProperties().forEach((k, v) -> producerProps.put(k.toString(), v));
        producerProps.put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        producerProps.put(AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, "true");
        producerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), avroSerializer),
                true, producerProps
        );
    }
}
