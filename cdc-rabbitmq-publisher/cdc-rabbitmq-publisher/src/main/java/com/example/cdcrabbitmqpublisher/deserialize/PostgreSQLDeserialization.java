package com.example.cdcrabbitmqpublisher.deserialize;

import com.example.cdcrabbitmqpublisher.model.DataChangeInfo;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;

/**
 * ClassName: PostgreSQLDeserialization
 * Package: com.example.cdcrabbitmqpublisher.deserialize
 * Description:
 *
 * @Author pwq
 * @Create 2025/3/20 11:43
 * @Version 1.0
 */
@Component
@Slf4j
public class PostgreSQLDeserialization implements DebeziumDeserializationSchema<DataChangeInfo>  {

    public static final String TS_MS = "ts_ms";
    public static final String DATABASE = "db";
    public static final String SCHEMA = "schema";
    public static final String TABLE = "table";
    public static final String BEFORE = "before";
    public static final String AFTER = "after";
    public static final String SOURCE = "source";

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<DataChangeInfo> collector) throws Exception {
        final String topic = sourceRecord.topic();
        log.info("收到{}的消息，准备进行转换", topic);

        final DataChangeInfo dataChangeInfo = new DataChangeInfo();

        final Struct struct = (Struct) sourceRecord.value();
        final Struct source = struct.getStruct(SOURCE);
        dataChangeInfo.setBefore(getDataJson(struct, BEFORE).toJSONString());
        dataChangeInfo.setAfter(getDataJson(struct, AFTER).toJSONString());

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        dataChangeInfo.setEventType(operation.toString().toLowerCase());
        dataChangeInfo.setDatabaseName(Optional.ofNullable(source.get(DATABASE)).map(Object::toString).orElse(""));
        dataChangeInfo.setSchemaName(Optional.ofNullable(source.get(SCHEMA)).map(Object::toString).orElse(""));
        dataChangeInfo.setTableName(Optional.ofNullable(source.get(TABLE)).map(Object::toString).orElse(""));
        dataChangeInfo.setChangeTime(Optional.ofNullable(struct.get(TS_MS)).map(x -> Long.parseLong(x.toString())).orElseGet(System::currentTimeMillis));

        //输出数据
        collector.collect(dataChangeInfo);
    }

    /**
     * 从元数据获取出变更之前或之后的数据
     */
    private JSONObject getDataJson(Struct value, String fieldElement) {
        Struct element = value.getStruct(fieldElement);
        JSONObject jsonObject = new JSONObject();
        if (element != null) {
            Schema afterSchema = element.schema();
            List<Field> fieldList = afterSchema.fields();
            for (Field field : fieldList) {
                Object afterValue = element.get(field);
                jsonObject.put(field.name(), afterValue);
            }
        }
        return jsonObject;
    }




    //Flink 需要 TypeInformation 来序列化和优化数据流
    //getProducedType() 用于提供 DataChangeInfo 的类型信息，避免 Flink 的泛型擦除问题
    @Override
    public TypeInformation<DataChangeInfo> getProducedType() {
        return TypeInformation.of(DataChangeInfo.class);
    }
}
