/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.apache.kafka.patches;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Extends the {@link PatchedAbstractKafkaAvroDeserializer} overriding the deserialization
 * logic. Specifically uses a {@link TopicRecordNameBasedDeserializationContext} instead
 * of the default.
 *
 * Additionally, provides a cache for {@code schemaRegistry.getAllSubjectsById(schemaId)}
 * calls since the TopicRecordNamingStrategy requires making this call for every record it
 * processes.
 */
public abstract class TopicRecordNameBasedAbstractKafkaAvroDeserializer extends PatchedAbstractKafkaAvroDeserializer {

	private final DecoderFactory decoderFactory = DecoderFactory.get();

	private final Map<Integer, Collection<String>> subjectsCache = new BoundedConcurrentHashMap<>(1000);

	protected Object deserialize(String topic, Boolean isKey, byte[] payload, Schema readerSchema)
			throws SerializationException {
		if (schemaRegistry == null) {
			throw new InvalidConfigurationException(
					"SchemaRegistryClient not found. You need to configure the deserializer "
							+ "or use deserializer constructor with SchemaRegistryClient.");
		}
		if (payload == null) {
			return null;
		}

		TopicRecordNameBasedDeserializationContext context = new TopicRecordNameBasedDeserializationContext(topic,
				isKey, payload);
		return context.read(context.schemaFromRegistry().rawSchema(), readerSchema);
	}

	private String subjectName(String topic, Boolean isKey, AvroSchema schemaFromRegistry) {
		return isDeprecatedSubjectNameStrategy(isKey) ? null : getSubjectName(topic, isKey, null, schemaFromRegistry);
	}

	private String getSubjectByTopicAndId(String topic, int schemaId) {
		try {
			Collection<String> allSubjectsById = getAllSubjectsById(schemaId);
			Optional<String> first = allSubjectsById.stream().filter((subj) -> subj.startsWith(topic)).findFirst();
			return first.orElseThrow(() -> new SerializationException(String
					.format("Can not find any subjects for schema id %s which start with topic %s", schemaId, topic)));
		}
		catch (RestClientException | IOException ex) {
			// avro deserialization may throw AvroRuntimeException, NullPointerException,
			// etc
			throw new SerializationException(String.format(
					"Error deserializing Avro message for schema id %s and topic %s while retrieving all subjects for the id from the schema registry",
					schemaId, topic), ex);
		}
	}

	private Collection<String> getAllSubjectsById(int schemaId) throws IOException, RestClientException {
		Collection<String> cachedSubjects = subjectsCache.get(schemaId);
		if (cachedSubjects != null) {
			return cachedSubjects;
		}

		synchronized (this) {
			cachedSubjects = subjectsCache.get(schemaId);
			if (cachedSubjects != null) {
				return cachedSubjects;
			}

			final Collection<String> subjects = schemaRegistry.getAllSubjectsById(schemaId);
			subjectsCache.put(schemaId, subjects);
			return subjects;
		}
	}

	private static String getSchemaType(Boolean isKey) {
		if (isKey == null) {
			return "unknown";
		}
		else if (isKey) {
			return "key";
		}
		else {
			return "value";
		}
	}

	/**
	 * Copy from {@link io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer}, should be kept up to date with
	 * library changes
	 */
	class TopicRecordNameBasedDeserializationContext {

		private final String topic;

		private final Boolean isKey;

		private final ByteBuffer buffer;

		private final int schemaId;

		TopicRecordNameBasedDeserializationContext(final String topic, final Boolean isKey, final byte[] payload) {
			this.topic = topic;
			this.isKey = isKey;
			this.buffer = getByteBuffer(payload);
			this.schemaId = buffer.getInt();
		}

		AvroSchema schemaFromRegistry() {
			try {
				String subjectName = ((isKey == null || strategyUsesSchema(isKey))
						? getSubjectByTopicAndId(topic, schemaId) : getSubject());
				return (AvroSchema) schemaRegistry.getSchemaBySubjectAndId(subjectName, schemaId);
			}
			catch (IOException ex) {
				throw new SerializationException(
						"Error retrieving Avro " + getSchemaType(isKey) + " schema for id " + schemaId, ex);
			}
			catch (RestClientException ex) {
				String errorMessage = "Error retrieving Avro " + getSchemaType(isKey) + " schema for id " + schemaId;
				throw toKafkaException(ex, errorMessage);
			}
		}

		AvroSchema schemaForDeserialize() {
			try {
				return isDeprecatedSubjectNameStrategy(isKey) ? AvroSchemaUtils.copyOf(schemaFromRegistry())
						: (AvroSchema) schemaRegistry.getSchemaBySubjectAndId(getSubject(), schemaId);
			}
			catch (IOException ex) {
				throw new SerializationException(
						"Error retrieving Avro " + getSchemaType(isKey) + " schema for id " + schemaId, ex);
			}
			catch (RestClientException ex) {
				String errorMessage = "Error retrieving Avro " + getSchemaType(isKey) + " schema for id " + schemaId;
				throw toKafkaException(ex, errorMessage);
			}
		}

		String getSubject() {
			boolean usesSchema = strategyUsesSchema(isKey);
			return subjectName(topic, isKey, usesSchema ? schemaFromRegistry() : null);
		}

		String getContext() {
			return getContextName(topic);
		}

		String getTopic() {
			return topic;
		}

		boolean isKey() {
			return isKey;
		}

		int getSchemaId() {
			return schemaId;
		}

		Object read(Schema writerSchema) {
			return read(writerSchema, null);
		}

		Object read(Schema writerSchema, Schema readerSchema) {
			DatumReader<?> reader = getDatumReader(writerSchema, readerSchema);
			int length = buffer.limit() - 1 - idSize;
			if (writerSchema.getType().equals(Type.BYTES)) {
				byte[] bytes = new byte[length];
				buffer.get(bytes, 0, length);
				return bytes;
			}
			else {
				int start = buffer.position() + buffer.arrayOffset();
				try {
					Object result = reader.read(null,
							decoderFactory.binaryDecoder(buffer.array(), start, length, null));
					if (writerSchema.getType().equals(Type.STRING)) {
						return result.toString();
					}
					else {
						return result;
					}
				}
				catch (IOException | RuntimeException ex) {
					// avro deserialization may throw AvroRuntimeException,
					// NullPointerException, etc
					throw new SerializationException("Error deserializing Avro message for id " + schemaId, ex);
				}
			}
		}

	}

}
