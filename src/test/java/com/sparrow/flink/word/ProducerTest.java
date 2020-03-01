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

package com.sparrow.flink.word;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.Random;

public class ProducerTest {
    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("p001");
        producer.setNamesrvAddr("localhost:9876");
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        String[] words = "我爱北京天安门，这里是麻雀".split("");
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            String word = words[new Random().nextInt(words.length)];
            Message msg = new Message("flink-word-count",
                    "", "id_" + i,
                    (word).getBytes());
            try {
                producer.send(msg);
                Thread.sleep(5L);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
