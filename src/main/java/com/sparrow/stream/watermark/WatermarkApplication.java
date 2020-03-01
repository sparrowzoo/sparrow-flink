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

package com.sparrow.stream.watermark;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSource;
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;

public class WatermarkApplication {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "flink-watermark-1");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-watermark-1");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //必须在setStreamTimeCharacteristic之后
        env.getConfig().setAutoWatermarkInterval(1);


        // get input data
        SingleOutputStreamOperator sourceOperator = env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema(), consumerProps)).name("rocketmq-source");

        final OutputTag<WatermarkElement> lateOutputTag = new OutputTag("late-data", TypeInformation.of(WatermarkElement.class));
        SingleOutputStreamOperator sourceProcessOperator = sourceOperator.process(new ProcessFunction<Map, WatermarkElement>() {
            @Override
            public void processElement(Map in, Context ctx, Collector<WatermarkElement> out) {
                String value = (String) in.get("value");
                WatermarkElement watermarkElement = JSON.parseObject(value, WatermarkElement.class);
                out.collect(watermarkElement);
            }
        }).setParallelism(4);

        SingleOutputStreamOperator withTimestampsAndWatermarks = sourceProcessOperator.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<WatermarkElement>() {
            @Override
            public long extractTimestamp(WatermarkElement element, long previousElementTimestamp) {
                return element.getTimestamp();
            }

            @Override
            public Watermark checkAndGetNextWatermark(WatermarkElement lastElement, long extractedTimestamp) {
                Watermark watermark = lastElement.getTimestamp() != null ? new Watermark(extractedTimestamp) : null;
                System.out.println(Thread.currentThread().getId()+"-watermark"+DateFormatUtils.format(watermark.getTimestamp(),DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()));
                return watermark;
            }
        }).setParallelism(1);//必须是1

        SingleOutputStreamOperator windowOperator= withTimestampsAndWatermarks
                .timeWindowAll(Time.seconds(1))
                .allowedLateness(Time.seconds(1))
                .sideOutputLateData(lateOutputTag)
                .trigger(new EventTriggerProxy())
                .apply(new AllWindowFunction<WatermarkElement, WatermarkElement, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<WatermarkElement> values, Collector<WatermarkElement> out) throws Exception {
                        for (WatermarkElement watermarkElement : values) {
                            out.collect(watermarkElement);
                        }
                    }
                });


        windowOperator.addSink(new SinkFunction<WatermarkElement>() {
            @Override
            public void invoke(WatermarkElement value, Context context) throws Exception {
                System.out.println("TIME:" + DateFormatUtils.format(context.timestamp(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()));
                System.out.println("WATER:" + DateFormatUtils.format(context.currentWatermark(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()));
                System.out.println(value);
            }
        })
                .name("word-count-sink");

        //这里必须是windows operator
        DataStream lateStream = windowOperator.getSideOutput(lateOutputTag);
        lateStream.countWindowAll(1).process(new ProcessAllWindowFunction<WatermarkElement, WatermarkElement, Window>() {
            @Override
            public void process(Context context, Iterable<WatermarkElement> elements, Collector<WatermarkElement> out) throws Exception {
                for (WatermarkElement element : elements) {
                    out.collect(element);
                }
            }
        }).printToErr();
        // execute program
        env.execute("Streaming WordCount");
    }
}
