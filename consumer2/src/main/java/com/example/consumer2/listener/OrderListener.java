package com.example.consumer2.listener;

import java.io.IOException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.example.consumer2.bo.RocketmqConfig;
import com.example.consumer2.config.MyAllo;
import com.example.consumer2.socket.MyWebSocketServer2;
import com.example.consumer2.socket.MyWebSocketServer3;



@Component("orderL")
public class OrderListener {

    @Resource
    private RocketmqConfig rocketmqConfig;
    @Autowired
    MyAllo myAllo;


    @PostConstruct
    public void defaultConsumer() throws MQClientException {
        System.err.println("init OrderMQPushConsumer");
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("order");
        defaultMQPushConsumer.setNamesrvAddr(rocketmqConfig.getNamesrvAddr());
        defaultMQPushConsumer.subscribe("order", "*");
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        defaultMQPushConsumer.setAllocateMessageQueueStrategy(myAllo);
        defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                try {
                    consumeOrderlyContext.setAutoCommit(true);
                    for (MessageExt messageExt : list) {
                        System.out.println("顺序消费消息2: "
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