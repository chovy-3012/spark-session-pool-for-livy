package io.vergil.livy.sessionpool;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Stopwatch;
import io.vergil.livy.sessionpool.model.ListSessionResponse;
import io.vergil.livy.sessionpool.model.Session;
import io.vergil.livy.sessionpool.model.Statement;
import io.vergil.livy.sessionpool.utils.GuidUtils;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * api 参考： https://livy.incubator.apache.org/docs/latest/rest-api.html
 */
@Slf4j
public class LivyClient {
    public static final int QUERY_TIMEOUT = 2 * 3600;

    public static final String SESSION_NO_STARTED = "not_started";
    public static final String SESSION_STARTING = "starting";
    public static final String SESSION_IDLE = "idle";
    public static final String SESSION_BUSY = "busy";
    public static final String SESSION_SHUTTING_DOWN = "shutting_down";
    public static final String SESSION_ERROR = "error";
    public static final String SESSION_DEAD = "dead";
    public static final String SESSION_KILLED = "killed";
    public static final String SESSION_SUCCESS = "success";

    public static final String STATEMENT_RUNNING = "running";
    public static final String STATEMENT_WAITING = "waiting";
    public static final String STATEMENT_AVAILABLE = "available";
    public static final String STATEMENT_ERROR = "error";
    public static final String STATEMENT_CANCELLING = "cancelling";
    public static final String STATEMENT_CANCELLED = "cancelled";
    protected String url;
    public static final int CREATE_SESSION_TIMEOUT = 10 * 60;
    public static String NAME_PREFIX = null;
    private OkHttpClient okHttpClient;

    static {
        try {
            NAME_PREFIX = "MY_LIVY_" + InetAddress.getLocalHost().getHostName() + "@";
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public LivyClient(String livyUrl) {
        this.url = livyUrl;
        this.okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofMillis(120_0000))
                .readTimeout(Duration.ofMillis(120_000))
                .build();
    }

    //create session
    public Session createSession(String user,
                                 String driverMemory,
                                 int driverCores,
                                 String executorMemory,
                                 int executorCores,
                                 int minExecutors,
                                 int maxExecutors,
                                 String queue) throws IOException {
        JSONObject body = new JSONObject();
        body.put("proxyUser", user);
        body.put("driverMemory", driverMemory);
        body.put("driverCores", driverCores);
        body.put("executorMemory", executorMemory);
        body.put("executorCores", executorCores);
        body.put("numExecutors", minExecutors);
        body.put("queue", queue);
        body.put("heartbeatTimeoutInSecond", 30 * 60);
        body.put("name", NAME_PREFIX + GuidUtils.newGuild());
        //目前测试环境的0.5版本livy不支持配置spark动态资源参数
//        JSONObject sparkConf = new JSONObject();
//        sparkConf.put("spark.dynamicAllocation.enabled", true);
//        sparkConf.put("spark.dynamicAllocation.initialExecutors", minExecutors);
//        sparkConf.put("spark.dynamicAllocation.minExecutors", minExecutors);
//        sparkConf.put("spark.dynamicAllocation.maxExecutors", maxExecutors);
//        sparkConf.put("spark.dynamicAllocation.executorIdleTimeout", 60 * 10);
//        body.put("conf", sparkConf);
        RequestBody requestBody = RequestBody.create(MediaType.get("application/json"), body.toString());
        Request request = new Request.Builder()
                .url(url + "/sessions")
                .post(requestBody)
                .header("X-Requested-By", "DataQuery")
                .build();
        Response response = okHttpClient.newCall(request).execute();
        String responseContent = response.body().string();
        int code = response.code();
        if (!(code >= 200 && code < 300)) {
            throw new IOException("create session exception: " + responseContent);
        }
        Session session = JSONObject.parseObject(responseContent, Session.class);
        session.setLivyClient(this);
        //轮询等待
        Stopwatch sw = Stopwatch.createStarted();
        for (; ; ) {
            try {
                Thread.sleep(2000);
                log.info("waiting some time for creating session:{}", session.getId());
                //超时处理
                long elapsed = sw.elapsed(TimeUnit.SECONDS);
                if (elapsed > CREATE_SESSION_TIMEOUT) {
                    log.error("create session timeout(s) : " + CREATE_SESSION_TIMEOUT);
                    deleteSession(session.getId());
                    throw new IOException("create session timeout");
                }
                Session session_current = getSession(session.getId());
                if (StringUtils.equals(session_current.getState(), LivyClient.SESSION_IDLE)) {
                    return session_current;
                } else if (StringUtils.equals(session_current.getState(), LivyClient.SESSION_STARTING)) {
                    continue;
                } else {
                    deleteSession(session.getId());
                    log.error("create session error,state:{}", session_current.getState());
                }
            } catch (Exception e) {
                log.error("create session error:{}", e.getMessage());
                throw new IOException("create session error : " + e.getMessage());
            }
        }
    }

    //delete session
    public void deleteSession(String sessionId) throws IOException {
        Request request = new Request.Builder()
                .url(url + "/sessions/" + sessionId)
                .delete()
                .header("X-Requested-By", "DataQuery")
                .build();
        Response response = okHttpClient.newCall(request).execute();
        String responseContent = response.body().string();
        int code = response.code();
        if (!(code >= 200 && code < 300)) {
            throw new IOException("delete session exception: " + responseContent);
        }
    }

    //get session
    public Session getSession(String sessionId) throws IOException {
        Request request = new Request.Builder()
                .url(url + "/sessions/" + sessionId)
                .get()
                .header("X-Requested-By", "DataQuery")
                .build();
        Response response = okHttpClient.newCall(request).execute();
        String responseContent = response.body().string();
        int code = response.code();
        if (code == 404) {
            return null;
        }
        if (!(code >= 200 && code < 300)) {
            throw new IOException("get session exception: " + responseContent);
        }
        Session session = JSONObject.parseObject(responseContent, Session.class);
        return session;
    }

    //list session
    public ListSessionResponse listSession() throws IOException {
        Request request = new Request.Builder()
                .url(url + "/sessions/")
                .get()
                .header("X-Requested-By", "DataQuery")
                .build();
        Response response = okHttpClient.newCall(request).execute();
        String responseContent = response.body().string();
        int code = response.code();
        if (!(code >= 200 && code < 300)) {
            throw new IOException("list session exception: " + responseContent);
        }
        ListSessionResponse listSessionResponse = JSONObject.parseObject(responseContent, ListSessionResponse.class);
        return listSessionResponse;
    }

    //execute statement
    public Statement executeStatement(String sessionId, String code_, String kind) throws IOException {
        JSONObject body = new JSONObject();
        body.put("code", code_);
        body.put("kind", kind);
        RequestBody requestBody = RequestBody.create(MediaType.get("application/json"), body.toString());
        Request request = new Request.Builder()
                .url(url + "/sessions/" + sessionId + "/statements")
                .post(requestBody)
                .header("X-Requested-By", "DataQuery")
                .build();
        Response response = okHttpClient.newCall(request).execute();
        String responseContent = response.body().string();
        int code = response.code();
        if (!(code >= 200 && code < 300)) {
            throw new IOException("execute statement exception: " + responseContent);
        }
        Statement statement = JSONObject.parseObject(responseContent, Statement.class);
        return statement;
    }

    //get statement
    public Statement getStatement(String sessionId, String statementId) throws IOException {
        Request request = new Request.Builder()
                .url(url + "/sessions/" + sessionId + "/statements/" + statementId)
                .get()
                .header("X-Requested-By", "DataQuery")
                .build();
        Response response = okHttpClient.newCall(request).execute();
        String responseContent = response.body().string();
        int code = response.code();
        if (!(code >= 200 && code < 300)) {
            throw new IOException("get statement exception: " + responseContent);
        }
        Statement statement = JSONObject.parseObject(responseContent, Statement.class);
        return statement;
    }

    //cancel statement
    public void cancelStatement(String sessionId, String statementId) throws IOException {
        Request request = new Request.Builder()
                .url(url + "/sessions/" + sessionId + "/statements/" + statementId + "/cancel")
                .post(RequestBody.create(MediaType.get("application/json"), ""))
                .header("X-Requested-By", "DataQuery")
                .build();
        Response response = okHttpClient.newCall(request).execute();
        String responseContent = response.body().string();
        int code = response.code();
        if (!(code >= 200 && code < 300)) {
            throw new IOException("cancel statement exception: " + responseContent);
        }
    }

    //test
    public Statement testStatement(String sessionId) throws IOException {
        return executeStatement(sessionId, "show databases", "sql");
    }

    public Statement executeStatementSync(String sessionId, String code_, String kind) throws IOException, SQLException {
        Statement statement = executeStatement(sessionId, code_, kind);
        //轮询等待
        Stopwatch sw = Stopwatch.createStarted();
        for (; ; ) {
            Statement queryStatement = getStatement(sessionId, statement.getId());
            if (!StringUtils.equals(queryStatement.getState(), LivyClient.STATEMENT_WAITING) &&
                    !StringUtils.equals(queryStatement.getState(), LivyClient.STATEMENT_RUNNING)) {
                return queryStatement;
            }
            //超时处理
            long elapsed = sw.elapsed(TimeUnit.SECONDS);
            if (elapsed > QUERY_TIMEOUT) {
                log.error("execute statement timeout,livy session id:{},statement id:{},code:{}", sessionId, statement.getId(), code_);
                throw new SQLException("query timeout(s) : " + QUERY_TIMEOUT);
            }
            try {
                Thread.sleep(200);
            } catch (Exception e) {
            }
        }
    }
}
