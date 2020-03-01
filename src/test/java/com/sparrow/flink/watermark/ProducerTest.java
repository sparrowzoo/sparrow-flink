/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparrow.flink.watermark;

import com.alibaba.fastjson.JSON;
import com.sparrow.stream.watermark.WatermarkElement;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.io.IOException;
import java.text.ParseException;
import java.util.Random;

public class ProducerTest {
    public static void main(String[] args) throws ParseException, MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("p001");
        producer.setNamesrvAddr("localhost:9876");
        try {
            producer.start();
            //producer.createTopic("flink-watermark-one", "flink-watermark-one", 1);
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        String[] times = new String[]{
                "2020-02-01 15:00:09",
                "2020-02-01 15:00:08",
                "2020-02-01 15:00:05",
                "2020-02-01 15:00:07",
                "2020-02-01 15:00:06",
                "2020-02-01 15:00:14",
                "2020-02-01 15:00:55",
                "2020-02-01 15:00:56",
                "2020-02-01 15:00:57",
        };

        char c = '0';
        while (true) {
            try {
                System.out.println("enter any key");
                c = (char) System.in.read();
                if (c == '\n') {
                    continue;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            int i = Integer.valueOf(c + "");
            WatermarkElement watermarkElement = new WatermarkElement(i + "", DateUtils.parseDate(times[i], "yyyy-MM-dd HH:mm:ss").getTime());
            Message msg = new Message("flink-watermark-1",
                    "", "id_" + System.currentTimeMillis(),
                    JSON.toJSONBytes(watermarkElement));
            try {
                producer.send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
