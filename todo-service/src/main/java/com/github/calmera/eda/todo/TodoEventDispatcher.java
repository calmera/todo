package com.github.calmera.eda.todo;

import com.github.calmera.eda.todo.commands.CreateTodo;
import com.github.calmera.eda.todo.events.TodoCreated;
import com.github.calmera.eda.todo.state.Todo;
import com.github.calmera.eda.todo.state.TodoState;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class TodoEventDispatcher implements Processor<String, SpecificRecord, String, SpecificRecord> {
    private ProcessorContext<String, SpecificRecord> context;

    @Override
    public void init(ProcessorContext<String, SpecificRecord> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, SpecificRecord> record) {
        SpecificRecord result = null;

        if (record.value() instanceof CreateTodo cmd) {
            result = processCreateTodo(cmd);
        }

        if (result != null) {
            Record<String, SpecificRecord> resultRecord = new Record<>(record.key(), result, System.currentTimeMillis());
            resultRecord.headers().add("type", TodoCreated.class.getName().getBytes(StandardCharsets.UTF_8));

            context.forward(resultRecord);
        }
    }

    protected SpecificRecord processCreateTodo(CreateTodo cmd) {
        Todo todo = new Todo(cmd.getKey(), cmd.getLabel(), cmd.getDescription(), cmd.getDueDate(), TodoState.TODO);

        return new TodoCreated(cmd, todo);
    }
}
