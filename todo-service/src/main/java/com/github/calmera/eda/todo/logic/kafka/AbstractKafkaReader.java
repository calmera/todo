package com.github.calmera.eda.todo.logic.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

public abstract class AbstractKafkaReader {
    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public AbstractKafkaReader(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    protected KafkaStreams getKafkaStreams() {
        if (! streamsBuilderFactoryBean.isRunning())
            throw new IllegalStateException("the application is not running");

        return streamsBuilderFactoryBean.getKafkaStreams();
    }

    protected boolean isReady() {
        return streamsBuilderFactoryBean.isRunning();
    }


}
