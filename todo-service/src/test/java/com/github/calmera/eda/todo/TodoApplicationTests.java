package com.github.calmera.eda.todo;

import com.github.calmera.eda.todo.commands.CreateTodo;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@EmbeddedKafka(topics = { TodoApplicationTests.TOPIC_EVENTS }, bootstrapServersProperty = "confluent_broker_endpoint")
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
class TodoApplicationTests {
	static final String TOPIC_EVENTS = "todo";

	private final KafkaProperties kafkaProperties;
	private final String topic;

	@Autowired
	public TodoApplicationTests(KafkaProperties kafkaProperties, Environment env) {
		this.kafkaProperties = kafkaProperties;
		this.topic = env.getProperty("app.topics.todo.events");
	}

	@Test
	public void basicSending()
			throws Exception {


//		ConsumerRecord<String, Object> createTodoCommand = consumer.poll(1000);
//		ConsumerRecord<String, Object> todoCreatedEvent = consumer.poll(1000);
//		assertThat(todoCreatedEvent.value(), instanceOf(TodoCreated.class));


		//Given
		Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(
				String.join(",", kafkaProperties.getBootstrapServers())));

		//Create a kafka producer
		Producer<String, Object> producer = new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(), new StringSerializer(), new KafkaAvroSerializer()).createProducer();


		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(String.join(",", kafkaProperties.getBootstrapServers()), "testGroup", "true");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		//Create a Consumer client
		Consumer<String, Object> consumer = new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(), new StringDeserializer(),new KafkaAvroDeserializer()).createConsumer();
		consumer.subscribe(Collections.singleton(topic));

		//When
		String key = UUID.randomUUID().toString();
		producer.send(new ProducerRecord<>(topic, key, new CreateTodo(key, "Sample Todo Item", null, null)));
		producer.flush();

		//Then
		assertThat(producer).isNotNull();

		//And
		ConsumerRecords<String, Object> rec = consumer.poll(Duration.ofSeconds(3));
		Iterable<ConsumerRecord<String, Object>> records = rec.records(topic);
		Iterator<ConsumerRecord<String, Object>> iterator = records.iterator();

		if (!iterator.hasNext()) Assertions.fail();

		ConsumerRecord<String, Object> next = iterator.next();
		assertThat(next.value()).isEqualTo("TEST");
	}

	@Bean
	SchemaRegistryClient schemaRegistryClient() {
		MockSchemaRegistryClient client = new MockSchemaRegistryClient();

		return client;
	}
}
