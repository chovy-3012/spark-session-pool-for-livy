package io.vergil.livy.sessionpool;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.EvictionConfig;
import org.apache.commons.pool2.impl.EvictionPolicy;

public class ScheduleEvictionPolicy<T> implements EvictionPolicy<T> {

    @Override
    public boolean evict(final EvictionConfig config, final PooledObject<T> underTest,
                         final int idleCount) {
        long elapsed = System.currentTimeMillis() - underTest.getCreateTime();
        if (config.getIdleEvictTime() < elapsed) {
            return true;
        }
        return false;
    }
}
