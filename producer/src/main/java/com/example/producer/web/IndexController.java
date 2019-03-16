package com.example.producer.web;

import com.example.producer.bean.bo.RocketmqConfig;
import com.example.producer.config.CheckListen;
import com.example.producer.service.IndexService;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.LongAdder;

import static com.oracle.jrockit.jfr.ContentType.Bytes;

/**
 * @Auther: gaoyang
 * @Date: 2019/3/15 15:10
 * @Description:
 */
@RestController
public class IndexController {

    @Resource
    private IndexService indexService;
    @Resource
    RocketmqConfig rocketmqConfig;
    @Resource
    private DefaultMQProducer defaultMQProducer;
    @Resource
    private TransactionMQProducer transactionMQProducer;

    public static final LongAdder adder = new LongAdder();


    //同步发送
    @GetMapping("synchronized_send")
    public Object synchronizedSend(@RequestParam(required = true) String str) throws Exception {
        Message m = new Message("default", "tags", "1", str.getBytes("utf-8"));
        SendResult send = defaultMQProducer.send(m);
        return send;
    }

    //异步发送
    @GetMapping("async_send")
    public Object asyncSend(@RequestParam(required = true) String str) throws Exception {
        Message m = new Message("default", "tags", "1", str.getBytes("utf-8"));
        defaultMQProducer.send(m, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("发送成功");
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("发送失败");
            }
        });
        return "ok";
    }

    //同步发送-顺序队列
    @GetMapping("order_send")
    public Object orderSend() throws Exception {
        for (int i = 0; i < 10; i++) {
            Message m = new Message("order", "tags", "1", String.valueOf(i).getBytes("utf-8"));
            SendResult send = defaultMQProducer.send(m);
            System.out.println(send.getSendStatus().name());
        }
        return "ok";
    }

    //同步发送-普通队列
    @GetMapping("default_order_send")
    public Object defaultorderSend() throws Exception {
        for (int i = 0; i < 10; i++) {
            Message m = new Message("default", "tags", "1", String.valueOf(i).getBytes("utf-8"));
            SendResult send = defaultMQProducer.send(m);
            System.out.println(send.getSendStatus().name());
        }
        return "ok";
    }

    //事务队列  第一次回查时间   15s,1min,往后一直每分钟回查一次~~~~设置回查时间测试最快的要2分钟左右..
    @GetMapping("transaction_send")
    public Object transactionSend(@RequestParam(required = true) String str) throws Exception {
        TransactionSendResult transactionSendResult = indexService.transactionSend(str);
        return transactionSendResult;
    }

    @GetMapping("add")
    public void add(Integer a) {
        adder.reset();
        adder.add(a == null ? 0 : 1);
    }
}
