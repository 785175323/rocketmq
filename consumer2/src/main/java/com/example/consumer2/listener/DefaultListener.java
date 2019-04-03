package com.example.consumer2.listener;

import com.example.consumer2.bo.RocketmqConfig;
import com.example.consumer2.socket.MyWebSocketServer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

@Component("defalutL")
public class DefaultListener {

    @Resource
    private RocketmqConfig rocketmqConfig;

    @PostConstruct
    public void defaultConsumer() throws MQClientException {
        System.err.println("init defaultMQPushConsumer2");
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("default");
        defaultMQPushConsumer.setNamesrvAddr(rocketmqConfig.getNamesrvAddr());
        defaultMQPushConsumer.subscribe("default", "*");
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                    ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                MyWebSocketServer next = null;
                if (!CollectionUtils.isEmpty(MyWebSocketServer.webSocketSet)) {
                    next = MyWebSocketServer.webSocketSet.iterator().next();
                }
                try {
                    Thread.sleep(1000);
                    for (MessageExt messageExt : list) {
                        String msg = "普通消费消息2: "
                                + new String(messageExt.getBody())
                                + "  " + "topic:" + messageExt.getTopic()
                                + "   " + "tags:" + messageExt.getTags();
                        System.out.println(msg);
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