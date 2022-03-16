package com.github.calmera.eda.todo.logic.kafka;

import com.github.calmera.eda.todo.logic.ListResponse;
import com.github.calmera.eda.todo.logic.TodoReader;
import com.github.calmera.eda.todo.state.Todo;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

public class GlobalKafkaTodoReader extends AbstractKafkaReader implements TodoReader {

    public GlobalKafkaTodoReader(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        super(streamsBuilderFactoryBean);
    }

    @Override
    public ListResponse<Todo> all() {
        return getKafkaStreams().metadataForAllStreamsClients().parallelStream()
                .map(streamsMetadata -> String.format("http://%s:%d/todos", streamsMetadata.host(), streamsMetadata.port()))
                .map(url -> {
                    RestTemplate template = new RestTemplate();

                    try {
                        return (ListResponse<Todo>) template.getForObject(url, ListResponse.class);
                    } catch (RestClientException rce) {
                        return new ListResponse<Todo>(rce);
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
