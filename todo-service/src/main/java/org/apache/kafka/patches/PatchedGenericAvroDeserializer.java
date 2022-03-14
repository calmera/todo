/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.apache.kafka.patches;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * A schema-registry aware deserializer for reading data in "generic Avro" format.
 *
 * <p>
 * This deserializer assumes that the serialized data was written in the wire format
 * defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format.
 * It requires access to a Confluent Schema Registry endpoint, which you must
 * {@link PatchedGenericAvroDeserializer#configure(Map, boolean)} via the parameter
 * "schema.registry.url".
 * </p>
 *
 * <p>
 * See {@link GenericAvroSerializer} for its serializer counterpart.
 * </p>
 *
 * Patch for unreleased fix in 7.0.2. Didn't alter anything aside from plugging in the
 * {@link PatchedKafkaAvroDeserializer} instead of the default
 * {@link KafkaAvroDeserializer}.
 *
 * @see <a href=
 * "https://github.com/confluentinc/schema-registry/commit/aa3df9a1b0f96aabb4555a2950513a9985ea9bb2#diff-d9f889c4de359831045b577bc9e8a89d1b512a2fbe8798e3cf0e678635eb109f"></a>
 */
@InterfaceStability.Unstable
public class PatchedGenericAvroDeserializer extends GenericAvroDeserializer implements Deserializer<GenericRecord> {

	private final PatchedKafkaAvroDeserializer inner;

	public PatchedGenericAvroDeserializer() {
		inner = new PatchedKafkaAvroDeserializer();
	}

	/**
	 * For testing purposes only.
	 * @param client the schema registry client
	 */
	PatchedGenericAvroDeserializer(final SchemaRegistryClient client) {
		inner = new PatchedKafkaAvroDeserializer(client);
	}

	@Override
	public void configure(final Map<String, ?> deserializerConfig, final boolean isDeserializerForRecordKeys) {
		inner.configure(deserializerConfig, isDeserializerForRecordKeys);
	}

	@Override
	public GenericRecord deserialize(final String topic, final byte[] bytes) {
		return (GenericRecord) inner.deserialize(topic, bytes);
	}

	@Override
	public void close() {
		inner.close();
	}

}
