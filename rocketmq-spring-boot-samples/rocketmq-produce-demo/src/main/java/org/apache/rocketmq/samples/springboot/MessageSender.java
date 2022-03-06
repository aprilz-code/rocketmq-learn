package org.apache.rocketmq.samples.springboot;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.core.MessagePostProcessor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Controller
public class MessageSender {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;


    private static final String TOPIC = "topic";

    public void syncSend(){
        /**
         * 发送可靠同步消息 ,可以拿到SendResult 返回数据
         * 同步发送是指消息发送出去后，会在收到mq发出响应之后才会发送下一个数据包的通讯方式。
         * 这种方式应用场景非常广泛，例如重要的右键通知、报名短信通知、营销短信等。
         *
         * 参数1： topic:tag
         * 参数2:  消息体 可以为一个对象
         * 参数3： 超时时间 毫秒
         */
        SendResult result= rocketMQTemplate.syncSend("springboot-topic:tag","这是一条同步消息",10000);
        System.out.println(result);
    }

    /**
     * 发送 可靠异步消息
     * 发送消息后，不等mq响应，接着发送下一个数据包。发送方通过设置回调接口接收服务器的响应，并可对响应结果进行处理。
     * 异步发送一般用于链路耗时较长，对于RT响应较为敏感的业务场景，例如用户上传视频后通过启动转码服务，转码完成后通推送转码结果。
     *
     * 参数1： topic:tag
     * 参数2:  消息体 可以为一个对象
     * 参数3： 回调对象
     */
    public void asyncSend() throws Exception{

        rocketMQTemplate.asyncSend("springboot-topic:tag1", "这是一条异步消息", new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {

                System.out.println("回调sendResult:"+sendResult);
            }

            @Override
            public void onException(Throwable e) {
                System.out.println(e.getMessage());
            }
        });
        TimeUnit.SECONDS.sleep(100000);
    }

    /**
     * 发送单向消息
     * 参数1： topic:tag
     * 参数2:  消息体 可以为一个对象
     */
    public void sendOneWay(){

        rocketMQTemplate.sendOneWay("springboot-topic:tag1", "这是一条单向消息");
    }


    /**
     * 发送单向的顺序消息
     */
    public void sendOneWayOrderly(){
        for(int i=0;i<10;i++){

            rocketMQTemplate.sendOneWayOrderly("springboot-topic:tag1", "这是一条顺序消息"+i,"2673");
            rocketMQTemplate.sendOneWayOrderly("springboot-topic:tag1", "这是一条顺序消息"+i,"2673");
        }
    }

    /**
     * 延迟发送
     */
    public void sendDelay() {
        Message<String> msg = new GenericMessage<String>("我是一条延迟消息");
        rocketMQTemplate.asyncSend("topic-delay", msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                //发送成功
            }
            @Override
            public void onException(Throwable throwable) {
                //发送失败
            }
        }, 100, 4);
        // messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        // 这里设置4，即30s的延迟


    }

    /**
     * convertAndSend发送可扩展消息
     *
     */
    public void convertAndSend(){

        // tags null
        rocketMQTemplate.convertAndSend(TOPIC, "tag null");
        // tags empty, 证明 tag 要么有值要么null, 不存在 empty 的 tag
        rocketMQTemplate.convertAndSend(TOPIC + ":", "tag empty ?");
        // 只有 tag 没有 key
        rocketMQTemplate.convertAndSend(TOPIC + ":a", "tag a");
        rocketMQTemplate.convertAndSend(TOPIC + ":b", "tag b");
        // 有 property, 即 RocketMQ 基础 API 里面, Message(String topic, String tags, String keys, byte[] body) 里面的 key
        // rocketmq-spring-boot-starter 把 userProperty 和其他的一些属性都糅合在 headers 里面可, 具体可以参考 org.apache.rocketmq.spring.support.RocketMQUtil.addUserProperties
        // 获取某个自定义的属性的时候, 直接 headers.get("自定义属性key") 就可以了
        Map<String, Object> properties = new HashMap<>();
        properties.put("property", 1);
        properties.put("another-property", "你好");
        rocketMQTemplate.convertAndSend(TOPIC, "property 1", properties);
        rocketMQTemplate.convertAndSend(TOPIC + ":a", "tag a property 1", properties);
        rocketMQTemplate.convertAndSend(TOPIC + ":b", "tag b property 1", properties);
        properties.put("property", 5);
        rocketMQTemplate.convertAndSend(TOPIC, "property 5", properties);
        rocketMQTemplate.convertAndSend(TOPIC + ":a", "tag a property 5", properties);
        rocketMQTemplate.convertAndSend(TOPIC + ":c", "tag c property 5", properties);

        // 消息后置处理器, 可以在发送前对消息体和headers再做一波操作
        rocketMQTemplate.convertAndSend(TOPIC, "消息后置处理器", new MessagePostProcessor() {
            /**
             * org.springframework.messaging.Message
             */
            @Override
            public Message<?> postProcessMessage(Message<?> message) {
                Object payload = message.getPayload();
                MessageHeaders messageHeaders = message.getHeaders();
                return message;
            }
        });

        // convertAndSend 底层其实也是 syncSend

    }

}
