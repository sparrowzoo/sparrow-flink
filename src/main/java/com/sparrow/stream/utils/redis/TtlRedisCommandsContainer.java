package com.sparrow.stream.utils.redis;

import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;

public interface TtlRedisCommandsContainer extends RedisCommandsContainer {
    void expire(String key, int seconds);
}
