package com.github.calmera.eda.todo.logic;

import java.util.ArrayList;
import java.util.List;

public class ListResponse<T> {
    private final List<T> items;
    private final List<String> errors;

    public ListResponse() {
        this(new ArrayList<>(), new ArrayList<>());
    }

    public ListResponse(T... items) {
        this(List.of(items), List.of());
    }

    public ListResponse(String... errors) {
        this(List.of(), List.of(errors));
    }

    public ListResponse(List<T> items, List<String> errors) {
        this.items = items;
        this.errors = errors;
    }

    public List<T> getItems() {
        return items;
    }

    public List<String> getErrors() {
        return errors;
    }

    public boolean isComplete() {
        return errors.isEmpty();
    }
}
