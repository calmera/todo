package com.github.calmera.eda.todo.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(value = { "schema", "specificData" })
public class GenericKafkaMixin {
}
