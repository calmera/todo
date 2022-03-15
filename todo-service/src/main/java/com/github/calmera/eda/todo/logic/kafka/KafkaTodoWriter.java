package com.github.calmera.eda.todo.logic.kafka;

import com.github.calmera.eda.todo.commands.*;
import com.github.calmera.eda.todo.events.*;
import com.github.calmera.eda.todo.logic.TodoWriter;
import com.github.calmera.eda.todo.state.Todo;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.state.KeyValueStore;

public class KafkaTodoWriter implements TodoWriter {
    private final KeyValueStore<String, Object> todos;

    public KafkaTodoWriter(KeyValueStore<String, Object> stateStore) {
        this.todos = stateStore;
    }

    @Override
    public SpecificRecord apply(SpecificRecord sr) throws IllegalArgumentException, IllegalStateException {
        if (sr instanceof DeleteTodo cmd) return applyDelete(cmd);
        if (sr instanceof CreateTodo cmd) return applyCreate(cmd);
        if (sr instanceof FinishTodo cmd) return applyFinish(cmd);
        if (sr instanceof RestoreTodo cmd) return applyRestore(cmd);
        if (sr instanceof UpdateTodo cmd) return applyUpdate(cmd);

        // -- ignore unknown commands and events
        return null;
    }

    public SpecificRecord applyCreate(CreateTodo cmd) throws IllegalArgumentException, IllegalStateException {
        // -- check if there is already an entry with the given key
        Object obj = this.todos.get(cmd.getKey());
        if (obj != null) {
            return new CommandFailed(cmd, null, "ALREADY_EXISTS", "a todo entry with key " + cmd.getKey() + " already exists.");
        }

        // -- create the new entry
        Todo entry = new Todo(
                cmd.getKey(),
                cmd.getLabel(),
                cmd.getDescription(),
                cmd.getDueDate(),
                false
        );

        // -- write the result to the state store
        this.todos.put(cmd.getKey(), entry);

        // -- notify of success
        return new TodoCreated(cmd, entry);
    }

    public SpecificRecord applyFinish(FinishTodo cmd) throws IllegalArgumentException, IllegalStateException {
        // -- get the entry from the state store
        Object obj = this.todos.get(cmd.getKey());
        if (obj == null) {
            return new CommandFailed(cmd, null, "NOT_FOUND", "no todo entry with key " + cmd.getKey());
        }

        // -- cast the entry
        Todo entry = (Todo) obj;

        // -- mark as completed
        entry.setCompleted(true);

        // -- write the result to the state store
        this.todos.put(cmd.getKey(), entry);

        // -- notify of success
        return new TodoFinished(cmd, entry);
    }

    public SpecificRecord applyUpdate(UpdateTodo cmd) throws IllegalArgumentException, IllegalStateException {
        // -- check if there is already an entry with the given key
        Object obj = this.todos.get(cmd.getKey());
        if (obj == null) {
            return new CommandFailed(cmd, null, "NOT_FOUND", "no todo entry with key " + cmd.getKey());
        }

        // -- cast the entry
        Todo entry = (Todo) obj;

        // -- update the entry
        if (cmd.getLabel() != null) entry.setLabel(cmd.getLabel());
        if (cmd.getDescription() != null) entry.setDescription(cmd.getDescription());
        if (cmd.getDueDate() != null) entry.setDueDate(cmd.getDueDate());

        // -- write the result to the state store
        this.todos.put(cmd.getKey(), entry);

        // -- notify of success
        return new TodoUpdated(cmd, entry);
    }

    public SpecificRecord applyRestore(RestoreTodo cmd) throws IllegalArgumentException, IllegalStateException {
        // -- get the entry from the state store
        Object obj = this.todos.get(cmd.getKey());
        if (obj == null) {
            return new CommandFailed(cmd, null, "NOT_FOUND", "no todo entry with key " + cmd.getKey());
        }

        // -- cast the entry
        Todo entry = (Todo) obj;

        // -- mark as not completed
        entry.setCompleted(false);

        // -- write the result to the state store
        this.todos.put(cmd.getKey(), entry);

        // -- notify of success
        return new TodoRestored(cmd, entry);
    }

    public SpecificRecord applyDelete(DeleteTodo cmd) throws IllegalArgumentException, IllegalStateException {
        // -- get the entry from the state store
        Object obj = this.todos.get(cmd.getKey());
        if (obj == null) {
            return new CommandFailed(cmd, null, "NOT_FOUND", "no todo entry with key " + cmd.getKey());
        }

        // -- delete the entry
        this.todos.delete(cmd.getKey());

        // -- notify of success
        return new TodoDeleted(cmd);
    }
}
