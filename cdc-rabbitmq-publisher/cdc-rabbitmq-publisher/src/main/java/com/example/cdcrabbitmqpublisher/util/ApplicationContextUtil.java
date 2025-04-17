package com.example.cdcrabbitmqpublisher.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * ClassName: ApplicationContextUtil
 * Package: com.example.cdcrabbitmqpublisher.util
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 16:46
 * @Version 1.0
 */
@Component
public class ApplicationContextUtil implements ApplicationContextAware, Serializable {
    /**
     * 上下文
     */
    private static ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return context;
    }

    public static <T> T getBean(Class<T> beanClass) {
        return context.getBean(beanClass);
    }
}
