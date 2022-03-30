package com.github.calmera.eda.todo.api;

import com.github.calmera.eda.todo.commands.*;
import com.github.calmera.eda.todo.logic.ListResponse;
import com.github.calmera.eda.todo.logic.TodoReader;
import com.github.calmera.eda.todo.state.Todo;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;

import javax.websocket.server.PathParam;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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

    @GetMapping("/{key}")
    public Todo read(@PathVariable("key") String key, @RequestParam(value = "local", defaultValue = "false") boolean local) {
        if (local) {
            return this.localTodoReader.get(key);
        } else {
            return this.globalTodoReader.get(key);
        }
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
            kafkaTemplate.send("todos", cmd.getKey(), cmd).get(10, TimeUnit.SECONDS);
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/{key}")
    public ResponseEntity<String> update(@PathVariable("key") String key, @RequestBody UpdateTodo cmd) {
        if (cmd.getKey() != null && !cmd.getKey().equals(key)) {
            return new ResponseEntity<>("conflicting key in path vs payload", HttpStatus.BAD_REQUEST);
        }

        // -- make sure the key is set
        cmd.setKey(key);

        try {
            kafkaTemplate.send("todos", cmd.getKey(), cmd).get(10, TimeUnit.SECONDS);
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/{key}/finish")
    public ResponseEntity<String> finish(@PathVariable("key") String key) {
        FinishTodo cmd = FinishTodo.newBuilder().setKey(key).build();

        try {
            kafkaTemplate.send("todos", cmd.getKey(), cmd).get(10, TimeUnit.SECONDS);
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/{key}/restore")
    public ResponseEntity<String> restore(@PathVariable("key") String key) {
        RestoreTodo cmd = RestoreTodo.newBuilder().setKey(key).build();

        try {
            kafkaTemplate.send("todos", cmd.getKey(), cmd).get(10, TimeUnit.SECONDS);
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(@PathVariable("key") String key) {
        DeleteTodo cmd = DeleteTodo.newBuilder().setKey(key).build();

        try {
            kafkaTemplate.send("todos", cmd.getKey(), cmd).get(10, TimeUnit.SECONDS);
            return new ResponseEntity<>(HttpStatus.ACCEPTED);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
