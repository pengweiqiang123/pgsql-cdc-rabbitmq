package com.example.cdcrabbitmqpublisher.service.sqlGenerator;

import com.example.cdcrabbitmqpublisher.model.DataChangeInfo;
import com.example.cdcrabbitmqpublisher.util.JSONObjectUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.StringJoiner;

/**
 * ClassName: InsertSqlGeneratorServieImpl
 * Package: com.example.cdcrabbitmqpublisher.service.sqlGenerator
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 15:37
 * @Version 1.0
 */
@Slf4j
@Service("CREATE")
public class InsertSqlGeneratorServiceImpl implements SqlGeneratorService{
    @Override
    public String generatorSql(DataChangeInfo dataChangeInfo) {
        String afterData = dataChangeInfo.getAfter();
        Map<String, Object> afterDataMap = JSONObjectUtils.JsonToMap(afterData);
        StringJoiner columnSetPart = new StringJoiner(",");
        StringJoiner valuePart = new StringJoiner(",");

        for (String key : afterDataMap.keySet()) {
            Object afterValue = afterDataMap.get(key);
            if ("create_time".equals(key)){
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                afterValue = dateFormat.format(afterValue);
            }
            columnSetPart.add(quoteIdentifier(key));
            valuePart.add(formatValue(afterValue));
        }
        log.info("columnSetPart : {}", columnSetPart);
        log.info("valuePart : {}", valuePart);
        return "INSERT INTO " + dataChangeInfo.getTableName() + "(" + columnSetPart + ") VALUES("
                + valuePart + ");";
    }
    public String quoteIdentifier(String identifier) {
        // 对字段名进行转义处理，这里简化为对其加反引号
        // 实际应该处理数据库标识符的特殊字符
        return "`" + identifier + "`";
    }

    public String formatValue(Object value) {
        // 根据值的类型返回 SQL 语句中的字符串
        // 这里需要根据实际类型来转换
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        } else if (value instanceof Number) {
            return value.toString();
        }else {
            return value.toString();
        }
    }
}
