package com.sparrow.stream.word;

import com.sparrow.stream.po.WordCount;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see org.apache.flink.streaming.connectors.redis.RedisSink
 */
public class FlinkRedisMapper implements RedisMapper<WordCount> {
    private static Logger logger = LoggerFactory.getLogger("word-count");

    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "demo:flink:word.count");
    }

    /**
     * 获取 value值 value的数据是键值对
     *
     * @param data
     * @return
     */
    public String getKeyFromData(WordCount data) {
        String key =data.getWord();
        logger.info("key {}", key);
        return key;
    }

    //指定value
    public String getValueFromData(WordCount data) {
        logger.info("value {}", data);
        return data.getCount()+"";
    }
}