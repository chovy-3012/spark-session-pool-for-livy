package io.vergil.livy.sessionpool;

import io.vergil.livy.sessionpool.model.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 设计：LivySessionPool使用commons-pool2进行实现
 * <p>
 * <p>
 * TODO 说明
 * LivySessionPool当前设计是单机模式，即每个实例都会创建自己的session pool
 * 1.当前这么做的原因：取数使用人较少，使用资源也较少，单机模式实现最简单
 * 2.缺点：当部署实例较多的时候会浪费很多资源，毕竟livypool有一些资源是闲置状态，实例越多闲置资源越多。
 * 因为请求用户不多，所以线上最多部署两个实例即可。
 * 如果实现多实例共享session pool，实现较为复杂，需要使用分布式锁，可放在以后实现。
 * <p>
 * 所有实例共享session pool。
 * 修改方法：在livyfactory中，分布式加锁初始化读取，分布式加锁实现create,分布式加锁destroy
 */
@Slf4j
public class LivySessionPool extends GenericObjectPool<Session> {
    private static GenericObjectPoolConfig defaultPoolConfig = new GenericObjectPoolConfig();
    //private static AbandonedConfig defaultAbandonedConfig = new AbandonedConfig();

    static {
        defaultPoolConfig.setMaxIdle(5);
        defaultPoolConfig.setMaxTotal(10);
        defaultPoolConfig.setFairness(true);
        defaultPoolConfig.setMaxWaitMillis(2000);
        //defaultAbandonedConfig.setRemoveAbandonedOnMaintenance(true);
        //defaultAbandonedConfig.setRemoveAbandonedOnBorrow(true);
        //defaultAbandonedConfig.setRemoveAbandonedTimeout(30);
    }

    public LivySessionPool(PooledObjectFactory<Session> factory) {
        super(factory, defaultPoolConfig);
    }

    public LivySessionPool(PooledObjectFactory<Session> factory, GenericObjectPoolConfig config) {
        super(factory, config);
    }

    public LivySessionPool(PooledObjectFactory<Session> factory, GenericObjectPoolConfig config, AbandonedConfig defaultAbandonedConfig) {
        super(factory, config);
    }

    private AtomicInteger numOfCreatingObject = new AtomicInteger(0);

    @Override
    public Session borrowObject() throws Exception {
        Session session = super.borrowObject();
        /*//为了减少初始化session的时间，当检测到idle的数量小于1，就预热一个session
        if (getNumIdle() < 1 && (getNumActive() + numOfCreatingObject.get()) < getMaxTotal()) {
            synchronized (this) {
                new Thread(() -> {
                    try {
                        numOfCreatingObject.addAndGet(1);
                        log.info("numIdle < minIdle ,prepare create session");
                        addObject();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        }*/
        return session;
    }
}
