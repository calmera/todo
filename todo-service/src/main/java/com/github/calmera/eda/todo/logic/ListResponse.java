package com.github.calmera.eda.todo.logic;

import java.util.List;

public class ListResponse<T> {
    private final List<T> items;
    private final List<Throwable> errors;

    public ListResponse() {
        this(List.of(), List.of());
    }

    public ListResponse(T... items) {
        this(List.of(items), List.of());
    }

    public ListResponse(Throwable... errors) {
        this(List.of(), List.of(errors));
    }

    public ListResponse(List<T> items, List<Throwable> errors) {
        this.items = items;
        this.errors = errors;
    }

    public List<T> getItems() {
        return items;
    }

    public List<Throwable> getErrors() {
        return errors;
    }

    public boolean isComplete() {
        return errors.isEmpty();
    }
}
