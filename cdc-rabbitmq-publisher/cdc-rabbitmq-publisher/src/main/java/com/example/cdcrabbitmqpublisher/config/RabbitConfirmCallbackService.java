package com.example.cdcrabbitmqpublisher.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

/**
 * ClassName: RabbitConfirmCallbackService
 * Package: com.example.cdcrabbitmqpublisher.config
 * Description:
 *
 * @Author pwq
 * @Create 2025/4/10 14:23
 * @Version 1.0
 */
@Service
@Slf4j
public class RabbitConfirmCallbackService implements RabbitTemplate.ConfirmCallback {


    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        if (ack) {
            log.info("消息发送成功至交换机!");
        } else {
            log.error("消息发送异常!");
            log.error("ConfirmCallback 相关数据: {}", correlationData);
            log.error("ConfirmCallback 确认情况: {}", ack);
            log.error("confirmCallback 失败原因: {}", cause);
        }
    }
}