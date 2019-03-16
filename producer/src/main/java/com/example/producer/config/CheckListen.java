package com.example.producer.config;

import com.example.producer.web.IndexController;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


@Component
public class CheckListen implements TransactionListener {

    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        System.out.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        System.out.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        int i = IndexController.adder.intValue();
        return i == 0 ? LocalTransactionState.UNKNOW : LocalTransactionState.COMMIT_MESSAGE;
    }
}
