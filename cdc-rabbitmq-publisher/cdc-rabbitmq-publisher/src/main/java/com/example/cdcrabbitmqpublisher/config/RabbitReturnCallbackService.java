package com.example.cdcrabbitmqpublisher.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

/**
 * ClassName: RabbitReturnCallbackService
 * Package: com.example.cdcrabbitmqpublisher.config
 * Description:
 *
 * @Author pwq
 * @Create 2025/4/10 14:24
 * @Version 1.0
 */
@Service
@Slf4j
public class RabbitReturnCallbackService implements RabbitTemplate.ReturnsCallback {


    /**
     * 消息路由失败，回调
     * 消息(带有路由键routingKey)到达交换机，与交换机的所有绑定键进行匹配，匹配不到触发回调
     */
    @Override
    public void returnedMessage(ReturnedMessage returned) {
        log.error("ReturnCallback 消息: " + returned.getMessage());
        log.error("ReturnCallback 响应码: " + returned.getReplyCode());
        log.error("ReturnCallback 回应消息: " + returned.getReplyText());
        log.error("ReturnCallback 交换机: " + returned.getExchange());
        log.error("ReturnCallback 路由键: " + returned.getRoutingKey());
    }

}
