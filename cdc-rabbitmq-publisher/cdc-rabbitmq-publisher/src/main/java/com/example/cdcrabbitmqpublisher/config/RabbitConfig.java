package com.example.cdcrabbitmqpublisher.config;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;

/**
 * ClassName: RabbitConfig
 * Package: com.example.cdcrabbitmqpublisher.config
 * Description:
 *
 * @Author pwq
 * @Create 2025/4/10 14:21
 * @Version 1.0
 */
@Configuration
public class RabbitConfig {
    @Value("${spring.rabbitmq.addresses}")
    private String addresses;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;

    @Value("${spring.rabbitmq.virtual-host}")
    private String virtualHost;

    @Value("${spring.rabbitmq.publisher-returns}")
    private boolean publisherReturns;

    @Autowired
    private RabbitConfirmCallbackService confirmCallbackService;
    @Autowired
    private RabbitReturnCallbackService returnCallbackService;

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        // connectionFactory.setHost();
        // connectionFactory.setPort();
        connectionFactory.setAddresses(addresses);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost(virtualHost);
        connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        connectionFactory.setPublisherReturns(publisherReturns);

        return connectionFactory;
    }

    @Bean
    // 如果需要对 rabbitTemplate 设置不同的回调类，需要设置原型模式，不然回调类只能有一个
    @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE, proxyMode = ScopedProxyMode.TARGET_CLASS)
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        // 设置消布确认回调，即当消息达到交换机回调
        // 只有开启了 Mandatory 才能出发回调函数,无论消息吐送结果怎样都强制调用回调函数
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setConfirmCallback(confirmCallbackService);
        // 消息(带有 RoutingKey)到达交换机，与交换机的所有所有绑定的键进行匹配，匹配不到触发回调
        rabbitTemplate.setReturnsCallback(returnCallbackService);
        return rabbitTemplate;
    }
}
