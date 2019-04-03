package com.example.consumer.config;

import java.util.Arrays;
import java.util.List;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.stereotype.Component;

@Component
public class MyAllo implements AllocateMessageQueueStrategy {
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> list, List<String> cidAll) {
        for (MessageQueue m : list) {
            if (m.getQueueId() == 1) {
                return Arrays.asList(m);
            }
        }
        return null;
    }

    @Override
    public String getName() {
        return "myallo";
    }
}
