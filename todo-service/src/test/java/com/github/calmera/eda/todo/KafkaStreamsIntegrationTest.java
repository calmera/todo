/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package com.github.calmera.eda.todo;

import com.github.calmera.eda.TestUtils;
import com.github.calmera.eda.todo.commands.*;
import com.github.calmera.eda.todo.events.*;
import com.github.calmera.eda.todo.state.Todo;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@EmbeddedKafka(topics = { KafkaStreamsIntegrationTest.TOPIC_TODO_EVENTS, KafkaStreamsIntegrationTest.TOPIC_TODO_STATE},
		bootstrapServersProperty = "confluent_broker_endpoint")
/*
 * Disabling caching makes the test run faster, and more consistent behavior with the
 * TopologyTestDriver. More messages are produced on the output topic.
 */
@SpringBootTest(webEnvironment = RANDOM_PORT, classes = TodoApplication.class,
		properties = { "spring.kafka.streams.properties.commit.interval.ms=100",
				"spring.kafka.streams.cacheMaxSizeBuffering=0B", "spring.kafka.streams.cleanup.onShutdown=true",
				"spring.kafka.streams.properties.schema.registry.url=mock://127.0.0.1",
				"spring.main.allow-bean-definition-overriding=true" })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(TestGenericAvroSerdesConfig.class)
@ActiveProfiles("test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaStreamsIntegrationTest {

	static final String TOPIC_TODO_EVENTS = "todo";
	static final String TOPIC_TODO_STATE = "todo-changelog";

	static final String SCHEMA_CREATE_TODO = String.format("%s-com.github.calmera.eda.todo.commands.CreateTodo", TOPIC_TODO_EVENTS);

	@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	private KafkaAvroDeserializer avroDeserializer;

	@Autowired
	private KafkaAvroSerializer avroSerializer;

	@Autowired
	private SchemaRegistryClient mockSchemaRegistryClient;

	private KafkaTemplate<String, Object> eventProducer;
	private Consumer<String, Object> eventConsumer;

	@BeforeAll
	protected void prepare() throws Exception {
		// Additional schemas the application reads or writes and expects to be in the
		// registry
		TestUtils.registerSchemas(mockSchemaRegistryClient, TOPIC_TODO_EVENTS,
				CreateTodo.SCHEMA$,
				TodoCreated.SCHEMA$,
				DeleteTodo.SCHEMA$,
				TodoDeleted.SCHEMA$,
				UpdateTodo.SCHEMA$,
				TodoUpdated.SCHEMA$,
				FinishTodo.SCHEMA$,
				TodoFinished.SCHEMA$,
				RestoreTodo.SCHEMA$,
				TodoRestored.SCHEMA$
		);

		TestUtils.registerSchemas(mockSchemaRegistryClient, TOPIC_TODO_STATE, Todo.SCHEMA$);

		// -- event consumer
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("eventConsumer", "true",
				embeddedKafkaBroker);
		consumerProps.put(SCHEMA_REGISTRY_URL_CONFIG, "schema.registry.url");
		consumerProps.put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
		consumerProps.put(AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, "true");
		consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

		eventConsumer = new KafkaConsumer<String, Object>(consumerProps, Serdes.String().deserializer(), avroDeserializer);
		eventConsumer.subscribe(List.of(TOPIC_TODO_EVENTS));

		// -- event producer
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
		producerProps.put(SCHEMA_REGISTRY_URL_CONFIG, "schema.registry.url");
		producerProps.put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
		producerProps.put(AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, "true");
		producerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

		eventProducer = new KafkaTemplate<String, Object>(
				new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), avroSerializer),
				true, producerProps);
		eventProducer.setDefaultTopic(TOPIC_TODO_EVENTS);
	}

	private void sendEvent(String key, Object event) throws ExecutionException, InterruptedException, TimeoutException {
		ProducerRecord<String, Object> record = new ProducerRecord<>(TOPIC_TODO_EVENTS, key, event);
		record.headers().add("type", event.getClass().getName().getBytes(StandardCharsets.UTF_8));

		eventProducer.send(record).get(10, TimeUnit.SECONDS);
	}

	@Test
	@Order(1)
	@DisplayName("Given a CreateTodo command, a todo needs to be created and a TodoCreated needs to be returned")
	void processValidMessage() throws ExecutionException, InterruptedException, TimeoutException {
		// -- GIVEN
		String key = UUID.randomUUID().toString();
		CreateTodo cmd = new CreateTodo(key, "Sample Todo", null, null);
		sendEvent(key, cmd);

		final Iterable<ConsumerRecord<String, Object>> records = KafkaTestUtils
				.getRecords(eventConsumer, 5_000, 2).records(TOPIC_TODO_EVENTS);
		assertThat(records).hasSize(2);
		Iterator<ConsumerRecord<String, Object>> iterator = records.iterator();

		ConsumerRecord<String, Object> commandRecord = iterator.next();
		assertThat(commandRecord.headers().lastHeader("type").value()).asString()
				.isEqualTo(CreateTodo.class.getName());

		final ConsumerRecord<String, Object> eventRecord = iterator.next();
		assertThat(eventRecord.headers().lastHeader("type").value()).asString()
				.isEqualTo(TodoCreated.class.getName());

//		assertThat(validatedRecord.key()).isEqualTo("trade_1");
//		ValidatedEvent validatedEvent = genericRecordToPojoConverter.convert(validatedRecord.value());
//		assertThat(validatedEvent.getMessageIdentifier()).isEqualTo("messageId");
	}
}
