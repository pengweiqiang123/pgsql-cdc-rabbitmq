package com.example.cdcrabbitmqpublisher.service.sqlGenerator;

import com.example.cdcrabbitmqpublisher.model.DataChangeInfo;
import com.example.cdcrabbitmqpublisher.util.JSONObjectUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * ClassName: UpdateSqlGeneratorServiceImpl
 * Package: com.example.cdcrabbitmqpublisher.service.sqlGenerator
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 16:04
 * @Version 1.0
 */
@Service("UPDATE")
@Slf4j
public class UpdateSqlGeneratorServiceImpl implements SqlGeneratorService{
    @Override
    public String generatorSql(DataChangeInfo dataChangeInfo) {
        String beforeData = dataChangeInfo.getBefore();
        String afterData = dataChangeInfo.getAfter();
        Map<String, Object> beforeDataMap = JSONObjectUtils.JsonToMap(beforeData);
        Map<String, Object> afterDataMap = JSONObjectUtils.JsonToMap(afterData);

        StringBuilder updateSetPart = new StringBuilder();
        StringBuilder wherePart = new StringBuilder();
        for (String key : beforeDataMap.keySet()) {
            Object beforeValue = beforeDataMap.get(key);
            Object afterValue = afterDataMap.get(key);

            if ("create_time".equals(key)){
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                beforeValue = dateFormat.format(beforeValue);
            }
            if ("create_time".equals(key)){
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                afterValue = dateFormat.format(afterValue);
            }
            if (!beforeValue.equals(afterValue)) {
                // 如果字段值发生变化，则将其加入到更新列表
                if (updateSetPart.length() > 0) {
                    // 不是第一个更改的字段，增加逗号分隔
                    updateSetPart.append(", ");
                }
                updateSetPart.append(quoteIdentifier(key))
                        .append(" = ")
                        .append(formatValue(afterValue));
            } else {
                if (wherePart.length() > 0) {
                    // 不是第一个更改的字段，增加逗号分隔
                    wherePart.append(", ");
                }
                wherePart.append(quoteIdentifier(key))
                        .append(" = ")
                        .append(formatValue(beforeValue));
            }
        }
        log.info("updateSetPart : {}", updateSetPart);
        log.info("wherePart : {}", wherePart);
        // 构建完整 SQL
        return "UPDATE " + quoteIdentifier(dataChangeInfo.getTableName())
                + " SET " + updateSetPart
                + " WHERE " + wherePart + ";";
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
