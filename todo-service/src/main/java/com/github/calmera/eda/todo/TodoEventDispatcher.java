package com.github.calmera.eda.todo;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Component;

@Component
public class TodoEventDispatcher implements Processor<String, Object, String, Object> {
    private ProcessorContext<String, Object> context;

    @Override
    public void init(ProcessorContext<String, Object> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, Object> record) {

    }
}
