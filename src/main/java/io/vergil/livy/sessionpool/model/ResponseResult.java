package io.vergil.livy.sessionpool.model;

import lombok.Data;

import java.util.List;

@Data
public class ResponseResult {
    private Schema schema;
    private List<List<Object>> data;

    @Data
    public static class Schema {
        private List<Field> fields;
    }

    @Data
    public static class Field {
        private String name;
        private String type;
        private boolean nullable;
    }
}
