/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.apache.kafka.patches;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.*;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Patch for unreleased fix in 7.0.2. Didn't alter anything aside from applying the fix
 * linked below.
 *
 * @see <a href=
 * "https://github.com/confluentinc/schema-registry/commit/aa3df9a1b0f96aabb4555a2950513a9985ea9bb2#diff-d9f889c4de359831045b577bc9e8a89d1b512a2fbe8798e3cf0e678635eb109f"></a>
 */
public abstract class PatchedAbstractKafkaAvroDeserializer extends AbstractKafkaSchemaSerDe {

	private final DecoderFactory decoderFactory = DecoderFactory.get();

	protected boolean useSpecificAvroReader = false;

	protected boolean avroReflectionAllowNull = false;

	protected boolean avroUseLogicalTypeConverters = false;

	private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<>();

	private final Map<SchemaPair, DatumReader<?>> datumReaderCache = new ConcurrentHashMap<>();

	/**
	 * Sets properties for this deserializer without overriding the schema registry client
	 * itself. Useful for testing, where a mock client is injected.
	 * @param config the config
	 */
	protected void configure(KafkaAvroDeserializerConfig config) {
		configureClientProperties(config, new AvroSchemaProvider());
		useSpecificAvroReader = config.getBoolean(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG);
		avroReflectionAllowNull = config.getBoolean(KafkaAvroDeserializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG);
		avroUseLogicalTypeConverters = config
				.getBoolean(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG);
	}

	protected KafkaAvroDeserializerConfig deserializerConfig(Map<String, ?> props) {
		return new KafkaAvroDeserializerConfig(props);
	}

	protected KafkaAvroDeserializerConfig deserializerConfig(Properties props) {
		return new KafkaAvroDeserializerConfig(props);
	}

	/**
	 * Deserializes the payload without including schema information for primitive types,
	 * maps, and arrays. Just the resulting deserialized object is returned.
	 *
	 * <p>
	 * This behavior is the norm for Decoders/Deserializers.
	 * @param payload serialized data
	 * @return the deserialized object
	 */
	protected Object deserialize(byte[] payload) throws SerializationException {
		return deserialize(null, null, payload, null);
	}

	/**
	 * Just like single-parameter version but accepts an Avro schema to use for reading
	 * @param payload serialized data
	 * @param readerSchema schema to use for Avro read (optional, enables Avro projection)
	 * @return the deserialized object
	 */
	protected Object deserialize(byte[] payload, Schema readerSchema) throws SerializationException {
		return deserialize(null, null, payload, readerSchema);
	}

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

		DeserializationContext context = new DeserializationContext(topic, isKey, payload);
		return context.read(context.schemaFromRegistry().rawSchema(), readerSchema);
	}

	private Integer schemaVersion(String topic, boolean isKey, int id, String subject, AvroSchema schema, Object result)
			throws IOException, RestClientException {
		Integer version;
		if (isDeprecatedSubjectNameStrategy(isKey)) {
			subject = getSubjectName(topic, isKey, result, schema);
		}
		AvroSchema subjectSchema = (AvroSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
		version = schemaRegistry.getVersion(subject, subjectSchema);
		return version;
	}

	private String subjectName(String topic, boolean isKey, AvroSchema schemaFromRegistry) {
		return isDeprecatedSubjectNameStrategy(isKey) ? null : getSubjectName(topic, isKey, null, schemaFromRegistry);
	}

	/**
	 * Deserializes the payload and includes schema information, with version information
	 * from the schema registry embedded in the schema.
	 * @param topic the topic
	 * @param isKey whether it is a key or not
	 * @param payload the serialized data
	 * @return a GenericContainer with the schema and data, either as a
	 * {@link NonRecordContainer}, {@link org.apache.avro.generic.GenericRecord}, or
	 * {@link SpecificRecord}
	 */
	protected GenericContainerWithVersion deserializeWithSchemaAndVersion(String topic, boolean isKey, byte[] payload)
			throws SerializationException, InvalidConfigurationException {
		// Even if the caller requests schema & version, if the payload is null we cannot
		// include it.
		// The caller must handle this case.
		if (payload == null) {
			return null;
		}

		// Annotate the schema with the version. Note that we only do this if the schema +
		// version are requested, i.e. in Kafka Connect converters. This is critical
		// because that
		// code *will not* rely on exact schema equality. Regular deserializers *must not*
		// include
		// this information because it would return schemas which are not equivalent.
		//
		// Note, however, that we also do not fill in the connect.version field. This
		// allows the
		// Converter to let a version provided by a Kafka Connect source take priority
		// over the
		// schema registry's ordering (which is implicit by auto-registration time rather
		// than
		// explicit from the Connector).
		DeserializationContext context = new DeserializationContext(topic, isKey, payload);
		AvroSchema schema = context.schemaForDeserialize();
		Object result = context.read(schema.rawSchema(), null);

		try {
			Integer version = schemaVersion(topic, isKey, context.getSchemaId(), context.getSubject(), schema, result);
			if (schema.rawSchema().getType().equals(Type.RECORD)) {
				return new GenericContainerWithVersion((GenericContainer) result, version);
			}
			else {
				return new GenericContainerWithVersion(new NonRecordContainer(schema.rawSchema(), result), version);
			}
		}
		catch (IOException ex) {
			throw new SerializationException(
					"Error retrieving Avro " + getSchemaType(isKey) + " schema version for id " + context.getSchemaId(),
					ex);
		}
		catch (RestClientException ex) {
			String errorMessage = "Error retrieving Avro " + getSchemaType(isKey) + " schema version for id "
					+ context.getSchemaId();
			throw toKafkaException(ex, errorMessage);
		}
	}

	protected DatumReader<?> getDatumReader(Schema writerSchema, Schema readerSchema) {
		// normalize reader schema
		final Schema finalReaderSchema = getReaderSchema(writerSchema, readerSchema);
		SchemaPair cacheKey = new SchemaPair(writerSchema, finalReaderSchema);

		return datumReaderCache.computeIfAbsent(cacheKey, (schema) -> {
			boolean writerSchemaIsPrimitive = AvroSchemaUtils.getPrimitiveSchemas().values().contains(writerSchema);
			if (writerSchemaIsPrimitive) {
				GenericData genericData = new GenericData();
				if (avroUseLogicalTypeConverters) {
					AvroData.addLogicalTypeConversion(genericData);
				}
				return new GenericDatumReader<>(writerSchema, finalReaderSchema, genericData);
			}
			else if (useSchemaReflection) {
				return new ReflectDatumReader<>(writerSchema, finalReaderSchema);
			}
			else if (useSpecificAvroReader) {
				return new SpecificDatumReader<>(writerSchema, finalReaderSchema);
			}
			else {
				GenericData genericData = new GenericData();
				if (avroUseLogicalTypeConverters) {
					AvroData.addLogicalTypeConversion(genericData);
				}
				return new GenericDatumReader<>(writerSchema, finalReaderSchema, genericData);
			}
		});
	}

	/**
	 * Normalizes the reader schema, puts the resolved schema into the cache.
	 * <li>
	 * <ul>
	 * if the reader schema is provided, use the provided one
	 * </ul>
	 * <ul>
	 * if the reader schema is cached for the writer schema full name, use the cached
	 * value
	 * </ul>
	 * <ul>
	 * if the writer schema is primitive, use the writer one
	 * </ul>
	 * <ul>
	 * if schema reflection is used, generate one from the class referred by writer schema
	 * </ul>
	 * <ul>
	 * if generated classes are used, query the class referred by writer schema
	 * </ul>
	 * <ul>
	 * otherwise use the writer schema
	 * </ul>
	 * </li>
	 * @param writerSchema the writer schema
	 * @param readerSchema the reader schema
	 * @return the reader schema
	 */
	private Schema getReaderSchema(Schema writerSchema, Schema readerSchema) {
		if (readerSchema != null) {
			return readerSchema;
		}
		final boolean shouldSkipReaderSchemaCacheUsage = shouldSkipReaderSchemaCacheUsage(writerSchema);
		if (!shouldSkipReaderSchemaCacheUsage) {
			readerSchema = readerSchemaCache.get(writerSchema.getFullName());
		}
		if (readerSchema != null) {
			return readerSchema;
		}
		boolean writerSchemaIsPrimitive = AvroSchemaUtils.getPrimitiveSchemas().values().contains(writerSchema);
		if (writerSchemaIsPrimitive) {
			readerSchema = writerSchema;
		}
		else if (useSchemaReflection) {
			readerSchema = getReflectionReaderSchema(writerSchema);
			readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
		}
		else if (useSpecificAvroReader) {
			readerSchema = getSpecificReaderSchema(writerSchema);
			if (!shouldSkipReaderSchemaCacheUsage) {
				readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
			}
		}
		else {
			readerSchema = writerSchema;
		}
		return readerSchema;
	}

	private boolean shouldSkipReaderSchemaCacheUsage(Schema schema) {
		return useSpecificAvroReader
				&& (schema.getType() == Type.ARRAY || schema.getType() == Type.MAP || schema.getType() == Type.UNION);
	}

	@SuppressWarnings("unchecked")
	private Schema getSpecificReaderSchema(Schema writerSchema) {
		if (writerSchema.getType() == Type.ARRAY || writerSchema.getType() == Type.MAP
				|| writerSchema.getType() == Type.UNION) {
			return writerSchema;
		}
		Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
		if (readerClass == null) {
			throw new SerializationException("Could not find class " + writerSchema.getFullName()
					+ " specified in writer's schema whilst finding reader's " + "schema for a SpecificRecord.");
		}
		try {
			return readerClass.newInstance().getSchema();
		}
		catch (InstantiationException ex) {
			throw new SerializationException(writerSchema.getFullName() + " specified by the "
					+ "writers schema could not be instantiated to " + "find the readers schema.");
		}
		catch (IllegalAccessException ex) {
			throw new SerializationException(writerSchema.getFullName() + " specified by the "
					+ "writers schema is not allowed to be instantiated " + "to find the readers schema.");
		}
	}

	private Schema getReflectionReaderSchema(Schema writerSchema) {
		ReflectData reflectData = avroReflectionAllowNull ? ReflectData.AllowNull.get() : ReflectData.get();
		Class<?> readerClass = reflectData.getClass(writerSchema);
		if (readerClass == null) {
			throw new SerializationException("Could not find class " + writerSchema.getFullName()
					+ " specified in writer's schema whilst finding reader's " + "schema for a reflected class.");
		}
		return reflectData.getSchema(readerClass);
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

	class DeserializationContext {

		private final String topic;

		private final Boolean isKey;

		private final ByteBuffer buffer;

		private final int schemaId;

		DeserializationContext(final String topic, final Boolean isKey, final byte[] payload) {
			this.topic = topic;
			this.isKey = isKey;
			this.buffer = getByteBuffer(payload);
			this.schemaId = buffer.getInt();
		}

		AvroSchema schemaFromRegistry() {
			try {
				String subjectName = (isKey == null || strategyUsesSchema(isKey)) ? getContext() : getSubject();
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