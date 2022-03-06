package org.apache.rocketmq.samples.springboot.consumer.ad;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * MessageModel：集群模式；广播模式
 * ConsumeMode：顺序消费；无序消费
 */
@Component
@RocketMQMessageListener(topic = "springboot-topic",consumerGroup = "consumer-group",
        //selectorExpression = "tag1",selectorType = SelectorType.TAG,
        messageModel = MessageModel.CLUSTERING, consumeMode = ConsumeMode.CONCURRENTLY)
public class MessageConsumer implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt message) {

        //消费三次 都失败  则直接签收 
        if( message.getReconsumeTimes()==3)
        {
            // insert into  log ()  消费失败数据  记录日志
            return;
        }
        System.out.println("----------接收到rocketmq消息:" + new String(message.getBody()));
        // rocketmq会自动捕获异常回滚  (官方默认会重复消费16次)
        int a=1/0;
    }
}