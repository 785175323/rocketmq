package com.example.consumer2.listener;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.example.consumer2.bo.RocketmqConfig;
import com.example.consumer2.socket.MyWebSocketServer2;

@Component("defalutL22")
public class DefaultListener2 {

    @Resource
    private RocketmqConfig rocketmqConfig;

    @PostConstruct
    public void defaultConsumer() throws MQClientException {
        System.err.println("init defaultMQPushConsumer2");
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("default1");
        defaultMQPushConsumer.setNamesrvAddr(rocketmqConfig.getNamesrvAddr());
        defaultMQPushConsumer.subscribe("default2", "*");
        defaultMQPushConsumer.setMessageModel(MessageModel.BROADCASTING);
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                    ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                MyWebSocketServer2 next = null;
                if (!CollectionUtils.isEmpty(MyWebSocketServer2.webSocketSet)) {
                    next = MyWebSocketServer2.webSocketSet.iterator().next();
                }
                try {
                    for (MessageExt messageExt : list) {
                        System.out.println("普通消费消息-广播模式2: "
                                + new String(messageExt.getBody())
                                + "  " + "topic:" + messageExt.getTopic()
                                + "   " + "tags:" + messageExt.getTags());
                        if (next != null)
                            next.sendMessage(new String(messageExt.getBody()));

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