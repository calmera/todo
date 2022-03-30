package com.github.calmera.eda.todo.logic.kafka;

import com.github.calmera.eda.todo.logic.ListResponse;
import com.github.calmera.eda.todo.logic.TodoReader;
import com.github.calmera.eda.todo.state.Todo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

public class GlobalKafkaTodoReader extends AbstractKafkaReader implements TodoReader {

    public GlobalKafkaTodoReader(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        super(streamsBuilderFactoryBean);
    }

    @Override
    public Todo get(String key) {
        KeyQueryMetadata md = getKafkaStreams().queryMetadataForKey("todos", key, Serdes.String().serializer());

        String url = String.format("http://%s:%d/todos/%s?local=true", md.activeHost().host(), md.activeHost().port(), key);
        RestTemplate template = new RestTemplate();

        try {
            return template.getForObject(url, Todo.class);
        } catch (RestClientException rce) {
            throw new IllegalStateException(rce);
        }
    }

    @Override
    public ListResponse<Todo> all() {
        return getKafkaStreams().metadataForAllStreamsClients().parallelStream()
                .map(streamsMetadata -> String.format("http://%s:%d/todos?local=true", streamsMetadata.host(), streamsMetadata.port()))
                .map(url -> {
                    RestTemplate template = new RestTemplate();

                    try {
                        return (ListResponse<Todo>) template.getForObject(url, ListResponse.class);
                    } catch (RestClientException rce) {
                        return new ListResponse<Todo>(rce.getMessage());
                    }
                }).reduce(new ListResponse<>(), (r1, r2) -> {
                    if (r1 == null) {
                        r1 = new ListResponse<>();
                    }

                    r1.getItems().addAll(r2.getItems());
                    r1.getErrors().addAll(r2.getErrors());

                    return r1;
                });
    }

}
