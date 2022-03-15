package com.github.calmera.eda.todo.logic;

import org.apache.avro.specific.SpecificRecord;

public interface TodoWriter {
    /**
     * Apply the given command
     * @param cmd the command to apply
     * @return  the outcome of applying the command
     */
    SpecificRecord apply(SpecificRecord cmd) throws IllegalArgumentException, IllegalStateException;
}
