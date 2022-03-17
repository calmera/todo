package com.github.calmera.eda.todo.logic.kafka;

import com.github.calmera.eda.todo.logic.ListResponse;
import com.github.calmera.eda.todo.logic.TodoReader;
import com.github.calmera.eda.todo.state.Todo;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

public class LocalKafkaTodoReader extends AbstractKafkaReader implements TodoReader {
    private ReadOnlyKeyValueStore<String, Object> todos;

    public LocalKafkaTodoReader(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        super(streamsBuilderFactoryBean);
    }

    @Override
    public ListResponse<Todo> all() {
        ListResponse<Todo> result = new ListResponse<>();

        getStore().all().forEachRemaining(kv -> {
            if (! (kv.value instanceof Todo)) {
                result.getErrors().add("the entry is not a todo but a " + kv.value.getClass().getName());
                return;
            }

            result.getItems().add((Todo) kv.value);
        });

        return result;
    }

    private synchronized ReadOnlyKeyValueStore<String, Object> getStore() {
        if (this.todos == null) {
            this.todos = getKafkaStreams().store(StoreQueryParameters.fromNameAndType("todos", QueryableStoreTypes.keyValueStore()));
        }

        return this.todos;
    }

}
