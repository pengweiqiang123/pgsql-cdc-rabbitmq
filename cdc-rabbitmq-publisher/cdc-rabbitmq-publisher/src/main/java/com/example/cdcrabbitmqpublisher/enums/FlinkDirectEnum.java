package com.example.cdcrabbitmqpublisher.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * ClassName: FlinkDirectEnum
 * Package: com.example.cdcrabbitmqpublisher.enums
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/21 17:10
 * @Version 1.0
 */
@Getter
@RequiredArgsConstructor
public enum FlinkDirectEnum {

    FLINK_CDC_EXCHANGE("postgresql_exchange"),
    FLINK_CDC_QUEUE("table01"),
    FLINK_CDC_ROUTING_KEY("table01");
    private final String name;

}
