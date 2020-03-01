package com.sparrow.stream.utils.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;

public class TtlRedisSink<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.connectors.redis.RedisSink.class);

    /**
     * This additional key needed for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET}.
     * Other {@link RedisDataType} works only with two variable i.e. name of the list and value to be added.
     * But for {@link RedisDataType#HASH} and {@link RedisDataType#SORTED_SET} we need three variables.
     * <p>For {@link RedisDataType#HASH} we need hash name, hash key and element.
     * {@code additionalKey} used as hash name for {@link RedisDataType#HASH}
     * <p>For {@link RedisDataType#SORTED_SET} we need set name, the element and it's score.
     * {@code additionalKey} used as set name for {@link RedisDataType#SORTED_SET}
     */
    private String additionalKey;
    private RedisMapper<IN> redisSinkMapper;
    private RedisCommand redisCommand;

    private FlinkJedisPoolConfig flinkJedisPoolConfig;
    private TtlRedisCommandsContainer redisCommandsContainer;

    private int expireSeconds;


    /**
     * Creates a new {@link org.apache.flink.streaming.connectors.redis.RedisSink} that connects to the Redis server.
     *
     * @param flinkJedisPoolConfig The configuration of {@link FlinkJedisConfigBase}
     * @param redisSinkMapper      This is used to generate Redis command and key value from incoming elements.
     */
    public TtlRedisSink(FlinkJedisPoolConfig flinkJedisPoolConfig, RedisMapper<IN> redisSinkMapper, int expireSeconds) {
        Preconditions.checkNotNull(flinkJedisPoolConfig, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");
        Preconditions.checkNotNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null");

        this.flinkJedisPoolConfig = flinkJedisPoolConfig;
        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription = redisSinkMapper.getCommandDescription();
        this.redisCommand = redisCommandDescription.getCommand();
        this.additionalKey = redisCommandDescription.getAdditionalKey();
        this.expireSeconds = expireSeconds;
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel.
     * Depending on the specified Redis data type (see {@link RedisDataType}),
     * a different Redis command will be applied.
     * Available commands are RPUSH, LPUSH, SADD, PUBLISH, SET, PFADD, HSET, ZADD.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(IN input) throws Exception {
        String key = redisSinkMapper.getKeyFromData(input);
        String value = redisSinkMapper.getValueFromData(input);

        switch (redisCommand) {
            case RPUSH:
                this.redisCommandsContainer.rpush(key, value);
                break;
            case LPUSH:
                this.redisCommandsContainer.lpush(key, value);
                break;
            case SADD:
                this.redisCommandsContainer.sadd(key, value);
                break;
            case SET:
                this.redisCommandsContainer.set(key, value);
                this.redisCommandsContainer.expire(key, expireSeconds);
                break;
            case PFADD:
                this.redisCommandsContainer.pfadd(key, value);
                break;
            case PUBLISH:
                this.redisCommandsContainer.publish(key, value);
                break;
            case ZADD:
                this.redisCommandsContainer.zadd(this.additionalKey, value, key);
                break;
            case HSET:
                this.redisCommandsContainer.hset(this.additionalKey, key, value);
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
        }
    }

    /**
     * Initializes the connection to Redis by either cluster or sentinels or single server.
     *
     * @throws IllegalArgumentException if jedisPoolConfig, jedisClusterConfig and jedisSentinelConfig are all null
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Preconditions.checkNotNull(flinkJedisPoolConfig, "Redis cluster config should not be Null");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(flinkJedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(flinkJedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(flinkJedisPoolConfig.getMinIdle());
        JedisPool jedisCluster = new JedisPool(genericObjectPoolConfig,flinkJedisPoolConfig.getHost());
        this.redisCommandsContainer = new TtlRedisContainer(jedisCluster);
    }

    /**
     * Closes commands container.
     *
     * @throws IOException if command container is unable to close.
     */
    @Override
    public void close() throws IOException {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }
}

