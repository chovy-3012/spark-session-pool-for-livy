package io.vergil.livy.sessionpool.model;

import lombok.Data;

import java.util.List;

@Data
public class ListSessionResponse {
    private int from;
    private int total;
    private List<Session> sessions;
}
