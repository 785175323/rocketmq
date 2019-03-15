package com.example.producer.web;

import com.example.producer.bean.bo.RocketmqConfig;
import com.example.producer.service.IndexService;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;

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


    @Autowired
    private DefaultMQProducer defaultMQProducer;

    @GetMapping("add")
    public Object add(@RequestParam(required = true) String str) throws Exception {
        Message m = new Message("topic", "tags", "1", str.getBytes("utf-8"));
        defaultMQProducer.send(m, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("成功成功");
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("发送失败");
            }
        });
        return "ok";
    }

}
