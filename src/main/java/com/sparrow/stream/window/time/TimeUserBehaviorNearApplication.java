package com.sparrow.stream.window.time;

import com.alibaba.fastjson.JSON;
import com.sparrow.stream.window.behivior.UserBehaviorBO;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSource;
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class TimeUserBehaviorNearApplication {
    private static Logger logger = LoggerFactory.getLogger(TimeUserBehaviorNearApplication.class);


    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(60000);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
        // advanced options:
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //checkpoints have to complete within one minute, or are discarded ms
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "flink-click");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-click-count");

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema(), consumerProps))
                .name("rocketmq-source")
                .setParallelism(4)
                .process(new ProcessFunction<Map, UserBehaviorBO>() {
                    @Override
                    public void processElement(Map in, Context ctx, Collector<UserBehaviorBO> out) {
                        String value = (String) in.get("value");
                        UserBehaviorBO userBehavior = JSON.parseObject(value, UserBehaviorBO.class);
                        userBehavior.setCount(1);
                        out.collect(userBehavior);
                    }
                })
                .name("map-processor")
                .setParallelism(4)
                .keyBy(new KeySelector<UserBehaviorBO, Integer>() {
                    @Override
                    public Integer getKey(UserBehaviorBO o) throws Exception {
                        return o.getCompanyId();
                    }
                })
                .timeWindow(Time.seconds(60))
                .trigger(new TimeTriggerProxy())
                .reduce(new ReduceFunction<UserBehaviorBO>() {
                    @Override
                    public UserBehaviorBO reduce(UserBehaviorBO value1, UserBehaviorBO value2) throws Exception {
                        value1.setCount(value1.getCount() + value2.getCount());
                        System.out.println("reduce " + value1.toString());
                        return value1;
                    }
                }).name("collection apply")
                .addSink(new SinkFunction<UserBehaviorBO>() {
                    @Override
                    public void invoke(UserBehaviorBO value, Context context) throws Exception {
                        System.out.println("sink result:" + value);
                    }
                }).setParallelism(1).name("top-n-sink");

        try {
            env.execute("realtime-click-near-n");
        } catch (Exception e) {
            logger.error("real time clicik", e);
        }
    }
}

