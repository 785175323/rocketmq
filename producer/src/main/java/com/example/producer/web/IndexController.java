package com.example.producer.web;

import com.alibaba.fastjson.JSON;
import com.example.producer.bean.bo.RocketmqConfig;
import com.example.producer.config.CheckListen;
import com.example.producer.service.IndexService;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
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
            Message m = new Message("order", "order_0", i + "", ("order_0--" + i).getBytes("utf-8"));
            SendResult send = defaultMQProducer.send(m, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    int id = (int) o;
                   // int index = id % list.size();
                    return list.get(id);
                }
            }, 0);
        }

        for (int i = 0; i < 10; i++) {
            Message m = new Message("order", "order_0", i + "", ("order_1--" + i).getBytes("utf-8"));
            SendResult send = defaultMQProducer.send(m, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    int id = (int) o;
                    // int index = id % list.size();
                    return list.get(id);
                }
            }, 1);
        }
        return "ok";
    }

    //同步发送-普通队列  集群模式   一个进程下面启动多个Consumer 时 consumerGroup 名字不能一样，否则无法启动；通过consumerGroupName自动进行负载均衡
    @GetMapping("default_order_send")
    public Object defaultorderSend() throws Exception {
        for (int i = 0; i < 10; i++) {
            Message m = new Message("default", "tags", "1", String.valueOf(i).getBytes("utf-8"));
            SendResult send = defaultMQProducer.send(m);
        }
        return "ok";
    }

    //同步发送-普通队列  广播模式
    @GetMapping("default2_order_send")
    public Object default2orderSend() throws Exception {
        for (int i = 0; i < 10; i++) {
            Message m = new Message("default2", "tags", "1", String.valueOf(i).getBytes("utf-8"));
            SendResult send = defaultMQProducer.send(m);
        }
        return "ok";
    }

    //事务队列  第一次回查时间默认   15s,1min,往后一直每分钟回查一次
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
