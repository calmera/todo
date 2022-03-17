package com.github.calmera.eda.todo.api;

import com.github.calmera.eda.todo.commands.CreateTodo;
import com.github.calmera.eda.todo.logic.ListResponse;
import com.github.calmera.eda.todo.logic.TodoReader;
import com.github.calmera.eda.todo.state.Todo;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/todos")
public class TodoController {

    private final TodoReader localTodoReader;

    private final TodoReader globalTodoReader;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TodoController(TodoReader localTodoReader, TodoReader globalTodoReader, KafkaTemplate<String, Object> kafkaTemplate) {
        this.localTodoReader = localTodoReader;
        this.globalTodoReader = globalTodoReader;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping
    public ListResponse<Todo> findAll(@RequestParam(value = "local", defaultValue = "false") boolean local) {
        if (local) {
            return this.localTodoReader.all();
        } else {
            return this.globalTodoReader.all();
        }
    }

    @PostMapping
    public ResponseEntity<String> create(@RequestBody CreateTodo cmd) {
        if (cmd.getKey() == null) {
            return new ResponseEntity<>("no 'key' provided", HttpStatus.BAD_REQUEST);
        }

        if (cmd.getLabel() == null) {
            return new ResponseEntity<>("no 'label' provided", HttpStatus.BAD_REQUEST);
        }

        try {
            kafkaTemplate.send("todos", cmd).get(10, TimeUnit.SECONDS);
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
