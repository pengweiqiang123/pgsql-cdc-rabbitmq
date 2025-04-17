package com.example.cdcrabbitmqpublisher.listener;

import com.esotericsoftware.minlog.Log;
import com.example.cdcrabbitmqpublisher.deserialize.PostgreSQLDeserialization;
import com.example.cdcrabbitmqpublisher.model.DataChangeInfo;
import com.example.cdcrabbitmqpublisher.sink.DataChangeSink;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

/**
 * ClassName: PostgreSqlListener
 * Package: com.example.cdcrabbitmqpublisher.listener
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 16:10
 * @Version 1.0
 */
@Component
@Slf4j
public class PostgreSqlListener implements ApplicationRunner {

    private  final DataChangeSink dataChangeSink;

    public PostgreSqlListener(DataChangeSink dataChangeSink) {
        this.dataChangeSink = dataChangeSink;
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("开始作业");
        //使用 StreamExecutionEnvironment.getExecutionEnvironment() 创建 Flink 的流处理环境
//        try {
//            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            //设置并行度为 1
//            env.setParallelism(1);
//            DebeziumSourceFunction<DataChangeInfo> dataChangeInfoPostgresqlSource = buildDataChangeSource();
//            //将数据源添加到 Flink 环境中，生成 DataStream<DataChangeInfo> 数据流，数据源名称为 "postgresql-source"。
//            DataStream<DataChangeInfo> streamSource = env
//                    .addSource(dataChangeInfoPostgresqlSource, "postgresql-source")
//                    .setParallelism(1);
//            streamSource.print();
//            if (dataChangeSink == null) {
//                throw new IllegalStateException("Sink is not initialized!");
//            }
//            streamSource.addSink(dataChangeSink);
//            //调用 env.execute("postgresql-source")，启动 Flink 作业，作业名称为 "postgresql-source
//            env.execute("postgresql-source");
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw e;
//        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //设置并行度为 1
            env.setParallelism(1);
            env.enableCheckpointing(5 * 60 * 1000L);
            env.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(60).toMillis());
            DebeziumSourceFunction<DataChangeInfo> dataChangeInfoPostgresqlSource = buildDataChangeSource();
            //将数据源添加到 Flink 环境中，生成 DataStream<DataChangeInfo> 数据流，数据源名称为 "postgresql-source"。
            DataStream<DataChangeInfo> streamSource = env
                    .addSource(dataChangeInfoPostgresqlSource, "postgresql-source")
                    .setParallelism(1);
            streamSource.print();
            if (dataChangeSink == null) {
                throw new IllegalStateException("Sink is not initialized!");
            }
            streamSource.addSink(dataChangeSink);
            //调用 env.execute("postgresql-source")，启动 Flink 作业，作业名称为 "postgresql-source
            env.execute("postgresql-source");
    }

    private DebeziumSourceFunction<DataChangeInfo> buildDataChangeSource(){
        Properties properties = new Properties();
        // 指定连接器启动时执行快照的条件：****重要*****
        //initial- 连接器仅在没有为逻辑服务器名称记录偏移量时才执行快照。
        //always- 连接器每次启动时都会执行快照。
        //never- 连接器从不执行快照。
        //initial_only- 连接器执行初始快照然后停止，不处理任何后续更改。
        //exported- 连接器根据创建复制槽的时间点执行快照。这是一种以无锁方式执行快照的绝佳方式。
        //custom- 连接器根据snapshot.custom.class属性的设置执行快照
//        properties.setProperty("debezium.snapshot.mode", "initial");
        //properties.setProperty("snapshot.mode", "initial");
        // 好像不起作用使用slot.name
        properties.setProperty("debezium.slot.name", "pg_cdc3");
        properties.setProperty("debezium.slot.drop.on.stop", "false");
        properties.setProperty("debezium.publication.name", "dbz_publication");
        properties.setProperty("debezium.snapshot.mode", "never");
        properties.setProperty("snapshot.mode", "never");

        return PostgreSQLSource.<DataChangeInfo>builder()
                .hostname("192.168.137.143") // PostgreSQL 服务器
                .port(5433) // 端口
                .database("testdb") // 数据库名称
                .schemaList("public") // 监听 schema（可指定多个）
                .tableList(".*") // 监听的表
                .username("pgsql")
                .decodingPluginName("pgoutput") // 解析插件
                .debeziumProperties(properties)
                .password("102300")
                .deserializer(new PostgreSQLDeserialization()) // 解析数据
                .build();
    }

}
