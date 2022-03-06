package org.apache.rocketmq.samples.springboot.call;

/**
 * @author paulG
 * @since 2020/11/4
 **/
public class RocketmqSendCallbackBuilder {


    public static RocketmqSendCallback commonCallback() {
        return new RocketmqSendCallback();
    }

}
