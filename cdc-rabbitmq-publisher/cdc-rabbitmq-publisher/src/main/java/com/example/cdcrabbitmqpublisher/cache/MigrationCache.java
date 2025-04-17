package com.example.cdcrabbitmqpublisher.cache;

import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClassName: MigrationCache
 * Package: com.example.cdcrabbitmqpublisher.cache
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 16:45
 * @Version 1.0
 */

public class MigrationCache implements Serializable {
    public  static final Map<String, String> MIGRATION_TABLE_CACHE = new ConcurrentHashMap<>();
    public static final Map<String,Map<String,String>> TABLE_CACHE1 = new ConcurrentHashMap<>();
    public static final String EXCHANGE_NAME = "postgresql_exchange";
}
