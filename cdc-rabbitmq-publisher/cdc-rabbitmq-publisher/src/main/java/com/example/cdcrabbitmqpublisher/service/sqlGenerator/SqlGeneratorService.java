package com.example.cdcrabbitmqpublisher.service.sqlGenerator;

import com.example.cdcrabbitmqpublisher.model.DataChangeInfo;

/**
 * ClassName: SqlGeneratorService
 * Package: com.example.cdcrabbitmqpublisher.service.sqlGenerator
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 15:12
 * @Version 1.0
 */

public interface SqlGeneratorService {

    String generatorSql(DataChangeInfo dataChangeInfo);
}
