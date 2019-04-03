package com.example.producer.config;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.example.producer.web.IndexController;

@Component
public class CheckListen implements TransactionListener {

    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        String format = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        System.out.println(format);
        if (!CollectionUtils.isEmpty(MyWebSocketServer.webSocketSet)) {
            MyWebSocketServer.webSocketSet.forEach(w -> {
                try {
                    w.sendMessage("根据本地事务返回状态" + format);
                } catch (IOException ee) {
                    ee.printStackTrace();
                }
            });
        }
        int i = IndexController.adder.intValue();
        return i == 0 ? LocalTransactionState.UNKNOW : LocalTransactionState.COMMIT_MESSAGE;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        String format = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        if (!CollectionUtils.isEmpty(MyWebSocketServer.webSocketSet)) {
            MyWebSocketServer.webSocketSet.forEach(w -> {
                try {
                    w.sendMessage("回查" + format);
                } catch (IOException ee) {
                    ee.printStackTrace();
                }
            });
        }
        System.out.println(format);
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
