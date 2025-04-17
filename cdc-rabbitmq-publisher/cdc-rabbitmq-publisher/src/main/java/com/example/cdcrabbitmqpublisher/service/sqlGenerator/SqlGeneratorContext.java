package com.example.cdcrabbitmqpublisher.service.sqlGenerator;

import com.example.cdcrabbitmqpublisher.model.DataChangeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * ClassName: SqlGeneratorContext
 * Package: com.example.cdcrabbitmqpublisher.service.sqlGenerator
 * Description:
 *
 * @Author pwq
 * @Create 2025/4/17 16:42
 * @Version 1.0
 */
@Component
public class SqlGeneratorContext {
    private final Map<String, SqlGeneratorService> sqlGeneratorStrategyMap;


    public SqlGeneratorContext(Map<String, SqlGeneratorService> sqlGeneratorStrategyMap) {
        this.sqlGeneratorStrategyMap = sqlGeneratorStrategyMap;
    }

    /**
     * 根据事件类型动态选择对应的 SQL 生成器实现类
     * @param eventType 事件类型（如 CREATE、UPDATE、DELETE）
     * @param dataChangeInfo 数据变更对象
     * @return 对应事件类型生成的 SQL 语句
     */
    public String generateSql(String eventType, DataChangeInfo dataChangeInfo) {
        SqlGeneratorService sqlGenerator = sqlGeneratorStrategyMap.get(eventType.toUpperCase());
        if (sqlGenerator == null) {
            throw new UnsupportedOperationException("未支持的事件类型: " + eventType);
        }
        return sqlGenerator.generatorSql(dataChangeInfo);
    }
}
