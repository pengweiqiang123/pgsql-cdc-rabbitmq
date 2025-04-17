package com.example.cdcrabbitmqpublisher.service.sqlGenerator;

import com.example.cdcrabbitmqpublisher.model.DataChangeInfo;
import com.example.cdcrabbitmqpublisher.util.JSONObjectUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * ClassName: DeleteSqlGeneratorServiceImpl
 * Package: com.example.cdcrabbitmqpublisher.service.sqlGenerator
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 16:03
 * @Version 1.0
 */
@Service("DELETE")
@Slf4j
public class DeleteSqlGeneratorServiceImpl implements SqlGeneratorService{
    @Override
    public String generatorSql(DataChangeInfo dataChangeInfo) {
        String beforeData = dataChangeInfo.getBefore();
        Map<String, Object> beforeDataMap = JSONObjectUtils.JsonToMap(beforeData);
        StringBuilder wherePart = new StringBuilder();
        for (String key : beforeDataMap.keySet()) {
            Object beforeValue = beforeDataMap.get(key);
            if ("create_time".equals(key)){
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                beforeValue = dateFormat.format(beforeValue);
            }
            if (wherePart.length() > 0) {
                // 不是第一个更改的字段，增加逗号分隔
                wherePart.append(", ");
            }
            wherePart.append(quoteIdentifier(key)).append(" = ").append(formatValue(beforeValue));
        }
        log.info("wherePart : {}", wherePart);
        return "DELETE FROM " + dataChangeInfo.getTableName() + " WHERE " + wherePart;
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
