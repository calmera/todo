package com.github.calmera.eda.todo;

import com.github.calmera.eda.todo.events.TodoCreated;
import com.github.calmera.eda.todo.logic.TodoWriter;
import com.github.calmera.eda.todo.logic.kafka.KafkaTodoWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class TodoEventDispatcher implements Processor<String, SpecificRecord, String, SpecificRecord> {
    private ProcessorContext<String, SpecificRecord> context;

    private TodoWriter todoWriter;

    @Override
    public void init(ProcessorContext<String, SpecificRecord> context) {
        this.context = context;
        this.todoWriter = new KafkaTodoWriter(context.getStateStore("todos"));
    }

    @Override
    public void process(Record<String, SpecificRecord> record) {
        // -- apply the record to the todoWriter
        SpecificRecord result = this.todoWriter.apply(record.value());

        if (result != null) {
            Record<String, SpecificRecord> resultRecord = new Record<>(record.key(), result, System.currentTimeMillis());
            resultRecord.headers().add("type", TodoCreated.class.getName().getBytes(StandardCharsets.UTF_8));

            context.forward(resultRecord);
        }
    }
}
