package io.vergil.livy.sessionpool;

import com.google.common.base.Stopwatch;
import io.vergil.livy.sessionpool.model.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class LivySessionFactory extends BasePooledObjectFactory<Session> {
    private String sessionUser;
    private String sessionDriverMemory;
    private int sessionDriverCores;
    private String sessionExecutorMemory;
    private int sessionExecutorCores;
    private int sessionMinExecutors;
    private int sessionMaxExecutors;
    private String sessionQueue;
    private LivyClient livyClient;
    private LinkedList<Session> pastSessions;

    public LivySessionFactory(String url,
                              String user,
                              String driverMemory,
                              int driverCores,
                              String executorMemory,
                              int executorCores,
                              int minExecutors,
                              int maxExecutors,
                              String queue) {
        this.sessionUser = user;
        this.sessionDriverCores = driverCores;
        this.sessionDriverMemory = driverMemory;
        this.sessionExecutorCores = executorCores;
        this.sessionExecutorMemory = executorMemory;
        this.sessionMinExecutors = minExecutors;
        this.sessionMaxExecutors = maxExecutors;
        this.sessionQueue = queue;
        this.livyClient = new LivyClient(url);
        //init
        //get previous session from livy
        try {
            List<Session> sessionList = livyClient.listSession().getSessions().stream()
                    .filter(session -> session != null)
                    .filter(session -> session.getName() != null)
                    .filter(session -> session.getName().startsWith(LivyClient.NAME_PREFIX))
                    .map(session -> {
                        session.setLivyClient(livyClient);
                        return session;
                    }).collect(Collectors.toList());
            pastSessions = new LinkedList<>(sessionList);
        } catch (Exception e) {
            log.error("init livy session error : {}", e.getMessage());
            e.printStackTrace();
        }
    }


    @Override
    public Session create() throws IOException {
        //get from past sessions
        synchronized (this) {
            if (CollectionUtils.isNotEmpty(pastSessions)) {
                Session session = pastSessions.poll();
                log.info("create session from past : {}", session.getId());
                return session;
            }
        }
        //create new session
        Session session = livyClient.createSession(sessionUser,
                sessionDriverMemory,
                sessionDriverCores,
                sessionExecutorMemory,
                sessionExecutorCores,
                sessionMinExecutors,
                sessionMaxExecutors,
                sessionQueue);
        log.info("create session : {}", session.getId());
        //prepare 预热
        //livyClient.testStatement(session.getId());
        return session;
    }

    @Override
    public PooledObject<Session> wrap(Session session) {
        return new DefaultPooledObject<>(session);
    }

    @Override
    public void destroyObject(PooledObject<Session> p) throws Exception {
        super.destroyObject(p);
        livyClient.deleteSession(p.getObject().getId());
        log.info("delete livy session : {}", p.getObject().getId());
    }

    //校验session有效性
    @Override
    public boolean validateObject(PooledObject<Session> p) {
        try {
            Session session = livyClient.getSession(p.getObject().getId());
            if (session == null) {
                //session 不存在
                log.error("validate livy session error:session not exist, id:{}", p.getObject().getId());
                return false;
            }
            String state = session.getState();

            /*********************session 正常情况*****************/
            if (StringUtils.equals(state, LivyClient.SESSION_IDLE) ||
                    StringUtils.equals(state, LivyClient.SESSION_BUSY) ||
                    StringUtils.equals(state, LivyClient.SESSION_SUCCESS)) {
                log.debug("validate livy session,id:{},state:true", p.getObject().getId());
                return true;
            } else if (StringUtils.equals(state, LivyClient.SESSION_STARTING)) {
                /*********************session 还在创建*****************/
                //轮询等待
                Stopwatch sw = Stopwatch.createStarted();
                for (; ; ) {
                    try {
                        Thread.sleep(2000);
                        log.info("waiting some time for creating session");
                    } catch (Exception e) {
                    }
                    //超时处理
                    long elapsed = sw.elapsed(TimeUnit.SECONDS);
                    if (elapsed > LivyClient.CREATE_SESSION_TIMEOUT) {
                        log.info("create session timeout(s) : " + LivyClient.CREATE_SESSION_TIMEOUT);
                        livyClient.deleteSession(session.getId());
                        log.error("validate livy session,create session timeout,id:{}", p.getObject().getId());
                        return false;
                    }
                    Session session_current = livyClient.getSession(session.getId());
                    if (org.apache.commons.lang3.StringUtils.equals(session_current.getState(), LivyClient.SESSION_IDLE)) {
                        log.debug("validate livy session,id:{},state:true", p.getObject().getId());
                        return true;
                    } else if (org.apache.commons.lang3.StringUtils.equals(session_current.getState(), LivyClient.SESSION_STARTING)) {
                        continue;
                    } else {
                        livyClient.deleteSession(session.getId());
                        log.error("validate livy session error,id:{},state:{}", p.getObject().getId(), session_current.getState());
                        return false;
                    }
                }
            } else {
                /*********************session 其他情况均属异常*****************/
                log.error("validate livy session error,id:{},state:{}", p.getObject().getId(), state);
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

}
