/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparrow.stream.word;

import com.sparrow.stream.po.WordCount;
import com.sparrow.stream.utils.redis.TtlRedisSink;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSource;
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema;

import java.util.Map;
import java.util.Properties;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCount}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 */
public class WordCountApplication {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "flink-word-count");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-word-count");

        String redisClusterAddress = "127.0.0.1:6379";
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

//        FlinkJedisClusterConfig flinkJedisPoolConfig = new FlinkJedisClusterConfig.Builder()
//                .setMaxIdle(30000)
//                .setMinIdle(3)
//                .setTimeout(5000)
//                .setMaxRedirections(3)
//                .setMaxTotal(8)
//                .setNodes(AddressAssemble.assemble(redisClusterAddress)).build();
//

        // get input data
        DataStream<WordCount> counts = env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema(),
                consumerProps)).name("rocketmq-source")
                .setParallelism(4)
                .process(new ProcessFunction<Map, WordCount>() {
                    @Override
                    public void processElement(Map in, Context ctx, Collector<WordCount> out) {
                        String value = (String) in.get("value");
                        WordCount wordCount = new WordCount(value, 1);
                        out.collect(wordCount);
                    }
                });
        // split up the lines in pairs (2-tuples) containing: (word,1)
        //Specifying keys via field positions is only valid for tuple data types. Type: String
        //counts.keyBy(0).sum(1);
        counts
                .keyBy("word")
                .timeWindow(Time.seconds(1))
                .sum("count").addSink(new TtlRedisSink(flinkJedisPoolConfig, new FlinkRedisMapper(), 60 * 60 * 24))
                .name("word-count-sink");
        // execute program
        env.execute("Streaming WordCount");
    }
}
