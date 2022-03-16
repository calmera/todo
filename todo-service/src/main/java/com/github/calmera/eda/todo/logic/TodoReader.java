package com.github.calmera.eda.todo.logic;

import com.github.calmera.eda.todo.state.Todo;

public interface TodoReader {

    ListResponse<Todo> all();

}
