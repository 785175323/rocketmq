package com.example.producer.service;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;

/**
 * @Auther: gaoyang
 * @Date: 2019/3/15 15:10
 * @Description:
 */
@Service
public class IndexService {

    @Resource
    private TransactionMQProducer transactionMQProducer;

    @Transactional
    public TransactionSendResult transactionSend(String str) throws UnsupportedEncodingException, MQClientException {
        Message m = new Message("transaction", "tags", "1", str.getBytes("utf-8"));

        m.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES,"1");
        m.putUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,"120");

        TransactionSendResult transactionSendResult = transactionMQProducer.sendMessageInTransaction(m, null);
        return transactionSendResult;
    }
}
