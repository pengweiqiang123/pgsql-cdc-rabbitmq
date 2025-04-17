package com.example.cdcrabbitmqpublisher.config;

import com.example.cdcrabbitmqpublisher.deserialize.PostgreSQLDeserialization;
import com.example.cdcrabbitmqpublisher.model.DataChangeInfo;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * ClassName: FlinkCDCManager
 * Package: com.example.cdcrabbitmqpublisher.config
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/31 8:38
 * @Version 1.0
 */
@Component
public class FlinkCDCManager {
    private static Thread flinkThread;  // 用于存储 Flink 任务线程

    public static void restartFlinkCDC(Set<String> tables) {
        stopFlinkCDC(); // 先停止旧的 Flink 作业

        flinkThread = new Thread(() -> {
            try {
                System.out.println("启动 Flink CDC，监听表：" + tables);

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                DebeziumSourceFunction<DataChangeInfo> postgresSource = PostgreSQLSource.<DataChangeInfo>builder()
                        .hostname("192.168.137.143")
                        .port(5433)
                        .database("testdb")
                        .schemaList("public")
                        .tableList(tables.toArray(new String[0])) // **动态更新监听表**
                        .username("pgsql")
                        .password("102300")
                        .decodingPluginName("pgoutput")
                        .deserializer(new PostgreSQLDeserialization())
                        .build();

                DataStream<DataChangeInfo> stream = env.addSource(postgresSource, "PostgreSQL CDC");
                stream.print();

                env.execute("PostgreSQL CDC Listener");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        flinkThread.start();
    }

    public static void stopFlinkCDC() {
        if (flinkThread != null) {
            flinkThread.interrupt();  // 停止旧的 Flink 线程
        }
    }
}
