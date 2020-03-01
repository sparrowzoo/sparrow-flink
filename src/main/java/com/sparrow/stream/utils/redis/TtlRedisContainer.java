package com.sparrow.stream.utils.redis;

import org.apache.flink.streaming.connectors.redis.common.container.RedisContainer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TtlRedisContainer extends RedisContainer implements TtlRedisCommandsContainer {
    private final JedisPool jedisPool;

    /**
     * Initialize Redis command container for Redis cluster.
     *
     * @param jedisPool JedisCluster instance
     */
    public TtlRedisContainer(JedisPool jedisPool) {
        super(jedisPool);
        this.jedisPool = jedisPool;
    }

    @Override
    public void expire(String key, int seconds) {
        try (Jedis jedis = this.jedisPool.getResource()) {
            jedis.expire(key, seconds);
        } catch (Exception e) {
            throw e;
        }
    }
}
