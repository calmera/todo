/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.github.calmera.eda;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public final class TestUtils {

	private TestUtils() {
	}

	static {
		Locale.setDefault(new Locale("en")); // impacts the country code tests
	}

	public static String createStringFromFile(String path) {
		try (InputStream is = TestUtils.class.getResourceAsStream(path)) {
			return new String(is.readAllBytes(), StandardCharsets.UTF_8);
		}
		catch (Exception ex) {
			throw new IllegalArgumentException("Failed to load file " + path);
		}
	}

	public static void registerSchemas(SchemaRegistryClient client, String topic, Schema... schemas) throws Exception {
		for (Schema s : schemas) {
			client.register(String.format("%s-%s", topic, s.getFullName()), new AvroSchema(s));
		}
	}

	public static void registerSchemas(SchemaRegistryClient client, String topic, String... schemaFiles) throws Exception {
		Set<String> uniqueValues = new HashSet<>();
		for (String s : schemaFiles) {
			if (uniqueValues.add(s)) {
				AvroSchema avroSchema = new AvroSchema(createStringFromFile(s));
				client.register(String.format("%s-%s", topic, avroSchema.name()), avroSchema);
			}
		}
	}

	public static GenericRecordBuilder newGenericRecord(SchemaRegistryClient client, String subject) throws RestClientException, IOException {
		SchemaMetadata sm = client.getLatestSchemaMetadata(subject);
		ParsedSchema ps = client.getSchemaById(sm.getId());

		return new GenericRecordBuilder((Schema) ps.rawSchema());
	}

}
