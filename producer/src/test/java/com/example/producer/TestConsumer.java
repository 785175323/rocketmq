package com.example.producer;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

public class TestConsumer {


    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("order1");
        defaultMQPushConsumer.setNamesrvAddr("123.207.167.160:9876");
        defaultMQPushConsumer.subscribe("order", "order_0");
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        defaultMQPushConsumer.setMessageModel(MessageModel.CLUSTERING);
        defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                try {
                    Thread.sleep(100);
                    for (MessageExt messageExt : list) {
                        System.out.println("顺序消费消息1: "
                                + new String(messageExt.getBody())
                                + "  " + "topic:" + messageExt.getTopic()
                                + "   " + "tags:" + messageExt.getTags());
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                } catch (Exception e) {
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }
        });
        defaultMQPushConsumer.start();
    }
}
