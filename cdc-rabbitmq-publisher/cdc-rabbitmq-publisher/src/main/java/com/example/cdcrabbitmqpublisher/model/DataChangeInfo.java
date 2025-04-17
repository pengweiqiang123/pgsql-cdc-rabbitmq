package com.example.cdcrabbitmqpublisher.model;

import lombok.Data;

import java.io.Serializable;

/**
 * ClassName: DataChangeInfo
 * Package: com.example.cdcrabbitmqpublisher.model
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 11:40
 * @Version 1.0
 */
@Data
public class DataChangeInfo implements Serializable {
    private String databaseName;

    private String schemaName;

    private String tableName;

    private String eventType; // INSERT, UPDATE, DELETE

    private String before;    // 变更前数据

    private String after;     // 变更后数据

    private long changeTime; //更改时间
}
