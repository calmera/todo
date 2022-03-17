package com.github.calmera.eda.todo;

import com.github.calmera.eda.todo.api.GenericKafkaMixin;
import com.github.calmera.eda.todo.logic.TodoReader;
import com.github.calmera.eda.todo.logic.kafka.GlobalKafkaTodoReader;
import com.github.calmera.eda.todo.logic.kafka.LocalKafkaTodoReader;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG;

//@EnableWebMvc
@Configuration
public class ApiConfig implements WebMvcConfigurer {
    @Bean
    public TodoReader localTodoReader(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        return new LocalKafkaTodoReader(streamsBuilderFactoryBean);
    }

    @Bean
    public TodoReader globalTodoReader(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        return new GlobalKafkaTodoReader(streamsBuilderFactoryBean);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(KafkaProperties properties,
                                                       KafkaAvroSerializer avroSerializer) {

        Map<String, Object> producerProps = properties.buildProducerProperties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "rest-client");
        producerProps.put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        producerProps.put(AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, "true");
        producerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), avroSerializer),
                true, producerProps
        );
    }

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        for (HttpMessageConverter<?> converter : converters) {
            if (! (converter instanceof MappingJackson2HttpMessageConverter c))
                continue;

            c.getObjectMapper().addMixIn(SpecificRecord.class, GenericKafkaMixin.class);
        }
    }
}
