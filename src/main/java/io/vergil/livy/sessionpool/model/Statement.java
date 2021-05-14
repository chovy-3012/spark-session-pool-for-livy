package io.vergil.livy.sessionpool.model;

import lombok.Data;

@Data
public class Statement {
    private String id;
    private String state;
    private Output output;
    private double progress;
    private long started;
    private long completed;

    @Data
    public static class Output {
        private String status;
        private Integer execution_count;
        private String data;
        private String evalue;
    }
}
