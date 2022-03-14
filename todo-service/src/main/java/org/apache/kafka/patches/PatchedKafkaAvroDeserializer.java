/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.apache.kafka.patches;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Patch for unreleased fix in 7.0.2. Only altered the isKey parameter aside from applying
 * the fix linked below.
 *
 * @see <a href=
 * "https://github.com/confluentinc/schema-registry/commit/aa3df9a1b0f96aabb4555a2950513a9985ea9bb2#diff-d9f889c4de359831045b577bc9e8a89d1b512a2fbe8798e3cf0e678635eb109f"></a>
 *
 * The isKey parameter of the deserialize method has been changed to `false`. This is a
 * deviation from the PR mentioned above.
 */
public class PatchedKafkaAvroDeserializer extends TopicRecordNameBasedAbstractKafkaAvroDeserializer
		implements Deserializer<Object> {

	private boolean isKey;

	/**
	 * Constructor used by Kafka consumer.
	 */
	public PatchedKafkaAvroDeserializer() {

	}

	public PatchedKafkaAvroDeserializer(SchemaRegistryClient client) {
		schemaRegistry = client;
	}

	public PatchedKafkaAvroDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
		schemaRegistry = client;
		configure(deserializerConfig(props));
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.isKey = isKey;
		configure(new KafkaAvroDeserializerConfig(configs));
	}

	@Override
	public Object deserialize(String topic, byte[] bytes) {
		return deserialize(topic, false, bytes, null);
	}

	/**
	 * Pass a reader schema to get an Avro projection
	 * @param topic the topic
	 * @param bytes the bytes
	 * @param readerSchema the reader schema
	 * @return the deserialized object
	 */
	public Object deserialize(String topic, byte[] bytes, Schema readerSchema) {
		return deserialize(topic, null, bytes, readerSchema);
	}

	@Override
	public void close() {

	}

}
