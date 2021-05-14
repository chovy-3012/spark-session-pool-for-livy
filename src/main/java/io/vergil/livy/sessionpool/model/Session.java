package io.vergil.livy.sessionpool.model;

import io.vergil.livy.sessionpool.LivyClient;
import lombok.Data;

import java.io.IOException;
import java.sql.SQLException;

@Data
public class Session {
    private String id;
    private String appId;
    private String owner;
    private String name;
    private String proxyUser;
    private String state;
    private LivyClient livyClient;

    //delete session
    public void deleteSession() throws IOException {
        livyClient.deleteSession(id);
    }

    //get session
    public Session getSession() throws IOException {
        return livyClient.getSession(id);
    }

    //list session
    public ListSessionResponse listSession() throws IOException {
        return livyClient.listSession();
    }

    //execute statement
    public Statement executeStatement(String code_, String kind) throws IOException {
        return livyClient.executeStatement(id, code_, kind);
    }

    //get statement
    public Statement getStatement(String statementId) throws IOException {
        return livyClient.getStatement(id, statementId);
    }

    //cancel statement
    public void cancelStatement(String statementId) throws IOException {
        livyClient.cancelStatement(id, statementId);
    }

    //test
    public Statement testStatement() throws IOException {
        return livyClient.testStatement(id);
    }

    public Statement executeStatementSync(String code_, String kind) throws IOException, SQLException {
        return livyClient.executeStatementSync(id, code_, kind);
    }
}
