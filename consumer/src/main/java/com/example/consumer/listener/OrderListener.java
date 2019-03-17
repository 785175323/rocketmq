package com.example.consumer.listener;

import com.example.consumer.bo.RocketmqConfig;
import com.example.consumer.socket.MyWebSocketServer2;
import com.example.consumer.socket.MyWebSocketServer3;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueByMachineRoom;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component("orderL")
public class OrderListener {

    @Resource
    private RocketmqConfig rocketmqConfig;


    @PostConstruct
    public void defaultConsumer() throws MQClientException {
        System.err.println("init OrderMQPushConsumer");
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("order");
        defaultMQPushConsumer.setNamesrvAddr(rocketmqConfig.getNamesrvAddr());
        defaultMQPushConsumer.subscribe("order", "order_0");
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                try {
                    Thread.sleep(1000);
                    consumeOrderlyContext.setAutoCommit(true);
                    for (MessageExt messageExt : list) {
                        System.out.println("顺序消费消息1: "
                                + new String(messageExt.getBody())
                                + "  " + "topic:" + messageExt.getTopic()
                                + "   " + "tags:" + messageExt.getTags());
                        if(!CollectionUtils.isEmpty(MyWebSocketServer2.webSocketSet)){
                            MyWebSocketServer3.webSocketSet.forEach(w->{
                                try {
                                    w.sendMessage(new String(messageExt.getBody()));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            });
                        }
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