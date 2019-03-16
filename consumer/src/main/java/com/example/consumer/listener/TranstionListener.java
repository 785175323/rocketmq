package com.example.consumer.listener;

import com.example.consumer.bo.RocketmqConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

@Component("transtionL")
public class TranstionListener {

    @Resource
    private RocketmqConfig rocketmqConfig;

    @PostConstruct
    public void defaultConsumer() throws MQClientException {
        System.err.println("init transtionMQPushConsumer");
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("transaction");
        defaultMQPushConsumer.setNamesrvAddr(rocketmqConfig.getNamesrvAddr());
        defaultMQPushConsumer.subscribe("transaction", "*");
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                try {
                    for (MessageExt messageExt : list) {
                        System.out.println("事务消费消息: "
                                + new String(messageExt.getBody())
                                + "  " + "topic:" + messageExt.getTopic()
                                + "   " + "tags:" + messageExt.getTags());
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });
        defaultMQPushConsumer.start();
    }
}