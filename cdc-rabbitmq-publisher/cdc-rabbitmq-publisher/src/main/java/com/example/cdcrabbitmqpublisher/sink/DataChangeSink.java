package com.example.cdcrabbitmqpublisher.sink;

import com.example.cdcrabbitmqpublisher.model.DataChangeInfo;
import com.example.cdcrabbitmqpublisher.service.sqlGenerator.SqlGeneratorContext;
import com.example.cdcrabbitmqpublisher.util.ApplicationContextUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import static com.example.cdcrabbitmqpublisher.cache.MigrationCache.EXCHANGE_NAME;
import static com.example.cdcrabbitmqpublisher.cache.MigrationCache.MIGRATION_TABLE_CACHE;

/**
 * ClassName: DataChangeSink
 * Package: com.example.cdcrabbitmqpublisher.sink
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 15:06
 * @Version 1.0
 */
@Slf4j
@Component
public class DataChangeSink extends RichSinkFunction<DataChangeInfo> {

    transient RabbitTemplate rabbitTemplate;

    transient SqlGeneratorContext sqlGeneratorContext;
//    @Autowired
//    transient TableDataConvertService tableDataConvertService;

    public void invoke(DataChangeInfo value,Context context){
        log.info("收到变更原始数据:{}", value);
        //转换后发送到对应的MQ
        MIGRATION_TABLE_CACHE.put("table01","table01");
        if (MIGRATION_TABLE_CACHE.containsKey(value.getTableName())) {
            String routingKey = MIGRATION_TABLE_CACHE.get(value.getTableName());
            String sql = sqlGeneratorContext.generateSql(value.getEventType(), value);
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, routingKey, sql);
            //rabbitTemplate.convertAndSend(EXCHANGE_NAME, routingKey, tableDataConvertService.convertSqlByDataChangeInfo(value));
        }else{
            log.info("不存在这张表");
//            CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
//            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "table02", value.toString(), correlationData);
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "table02", value.toString());
        }

    }

    /**
     * 在启动SpringBoot项目是加载了Spring容器，其他地方可以使用@Autowired获取Spring容器中的类；但是Flink启动的项目中，
     * 默认启动了多线程执行相关代码，导致在其他线程无法获取Spring容器，只有在Spring所在的线程才能使用@Autowired，
     * 故在Flink自定义的Sink的open()方法中初始化Spring容器
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.rabbitTemplate = ApplicationContextUtil.getBean(RabbitTemplate.class);
        this.sqlGeneratorContext = ApplicationContextUtil.getBean(SqlGeneratorContext.class);
//        this.tableDataConvertService = ApplicationContextUtil.getBean(TableDataConvertService.class);
    }
}
