package io.vergil.livy.sessionpool;

import io.vergil.livy.sessionpool.model.Session;
import io.vergil.livy.sessionpool.model.Statement;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Before;
import org.junit.Test;

public class TestSessionPool {
    private LivySessionPool livySessionPool;

    //创建session
    @Before
    public void createSessionPool() throws Exception {
        //配置
        LivySessionFactory livySessionFactory = new LivySessionFactory("http://10.12.6.58:8999",
                "work",
                "2G",
                2,
                "2G",
                2,
                2,
                2,
                "default");

        //池子配置
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMinIdle(1);
        genericObjectPoolConfig.setMaxIdle(1);//保留连接池大小
        genericObjectPoolConfig.setMaxTotal(1);//最大连接池大小
        genericObjectPoolConfig.setMinEvictableIdleTimeMillis(1000 * 60 * 60 * 12);//空闲移除时长
        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(1000 * 60 * 1);//空间资源监测时间间隔
        genericObjectPoolConfig.setTestOnCreate(true);//验证。create
        genericObjectPoolConfig.setTestOnBorrow(true);//验证。borrow
        genericObjectPoolConfig.setTestWhileIdle(true);//验证。idle
        genericObjectPoolConfig.setFairness(true);//从池子中拿对象的公平锁
        genericObjectPoolConfig.setLifo(false);//last in first out，设置为false，可以让池子中的对象排队被获取
        genericObjectPoolConfig.setEvictionPolicyClassName(ScheduleEvictionPolicy.class.getName());//自定义实现驱逐策略，定时驱逐

        //创建session pool
        livySessionPool = new LivySessionPool(livySessionFactory, genericObjectPoolConfig);
        //预热
        livySessionPool.preparePool();
    }

    @Test
    public void executeStatementTest() throws Exception {
        Session session = livySessionPool.borrowObject();
        try {
            Statement statement = session.executeStatementSync("show databases", "sql");
            System.out.println("statement = " + statement);
        } catch (Exception e) {

        } finally {
            livySessionPool.returnObject(session);
        }
    }
}
