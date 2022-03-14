/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.github.calmera.eda.todo;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import org.apache.kafka.patches.PatchedGenericAvroDeserializer;
import org.apache.kafka.patches.PatchedMockSchemaRegistryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

// TODO: move to test-scoped lib
@Configuration
public class TestGenericAvroSerdesConfig {

	@Bean
	public static SchemaRegistryClient schemaRegistryClient() {
		return new PatchedMockSchemaRegistryClient();
	}

	@Bean
	public static KafkaAvroDeserializer avroDeserializer(SchemaRegistryClient schemaRegistryClient) {
		HashMap<String, String> props = new HashMap<>();
		props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

		return new KafkaAvroDeserializer(schemaRegistryClient, props);
	}

	@Bean
	public static KafkaAvroSerializer avroSerializer(SchemaRegistryClient schemaRegistryClient) {
		return new KafkaAvroSerializer(schemaRegistryClient);
	}

}
