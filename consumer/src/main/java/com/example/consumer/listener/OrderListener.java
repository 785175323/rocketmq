package com.example.consumer.listener;

import com.example.consumer.bo.RocketmqConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

@Component("orderL")
public class OrderListener {

    @Resource
    private RocketmqConfig rocketmqConfig;

    @PostConstruct
    public void defaultConsumer() throws MQClientException {
        System.err.println("init OrderMQPushConsumer");
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("order");
        defaultMQPushConsumer.setNamesrvAddr(rocketmqConfig.getNamesrvAddr());
        defaultMQPushConsumer.subscribe("order", "*");
        defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                try {
                    for (MessageExt messageExt : list) {
                        System.out.println("顺序消费消息: "
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