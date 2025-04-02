package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

public class QlikToDebeziumDirectTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        if (!(record.value() instanceof Map)) {
            throw new RuntimeException("Input message is not in expected Map format");
        }

        Map<String, Object> qlikMessage = (Map<String, Object>) record.value();
        Map<String, Object> newMessage = new HashMap<>();
        Map<String, Object> payload = new HashMap<>();

        Schema beforeSchema = SchemaBuilder.struct().build();
        Struct beforeStruct = new Struct(beforeSchema);
        // Copia toda a estrutura beforeData para before
        if (qlikMessage.containsKey("beforeData")) {

            Map<String, Object> beforeData = (Map<String, Object>) qlikMessage.get("beforeData");
            payload.put("before", beforeData);
            if (beforeData != null) {
                beforeSchema = createDynamicSchema(beforeData);
                beforeStruct = createDynamicStruct(beforeSchema, beforeData);
            }
        }

        Schema afterSchema = SchemaBuilder.struct().build();
        Struct afterStruct = new Struct(afterSchema);
        // Copia toda a estrutura data para after
        if (qlikMessage.containsKey("data")) {
            Map<String, Object> afterData = (Map<String, Object>) qlikMessage.get("data");
            payload.put("after", afterData);
            if (afterData != null) {
                afterSchema = createDynamicSchema(afterData);
                afterStruct = createDynamicStruct(afterSchema, afterData);
            }
        }

        String op = "c";
        // Copia a operação
        if (qlikMessage.containsKey("headers")) {
            Map<String, Object> headers = (Map<String, Object>) qlikMessage.get("headers");
            if (headers.containsKey("operation")) {
                Object operation = headers.get("operation");

                if (operation != null) {
                    if ("UPDATE".equals(operation.toString())) {
                        op = "u";
                    }
                    if ("DELETE".equals(operation.toString())) {
                        op = "d";
                    }
                }

                payload.put("op", op);
            }
        }

        newMessage.put("payload", payload);

        Schema payloadSchema = SchemaBuilder.struct().optional()
                .field("before", beforeSchema)
                .field("after", afterSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        Struct payloadStruct = new Struct(payloadSchema);

        payloadStruct.put("before", beforeStruct);
        payloadStruct.put("after", afterStruct);
        payloadStruct.put("op", op);

        Schema resultSchema = SchemaBuilder.struct().optional()
                .field("payload", payloadSchema)
                .build();

        Struct resultStruct = new Struct(resultSchema);

        resultStruct.put("payload", payloadStruct);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                newMessage,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    public Struct createDynamicStruct(Schema schema, Map<String, Object> data) {
        Struct struct = new Struct(schema);

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            struct.put(fieldName, fieldValue);
        }

        return struct;
    }

    public Schema createDynamicSchema(Map<String, Object> data) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().optional();

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            if (fieldValue instanceof Integer) {
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT32_SCHEMA);
            } else if (fieldValue instanceof Long) {
                schemaBuilder.field(fieldName, Schema.OPTIONAL_INT64_SCHEMA);
            } else if (fieldValue instanceof Double) {
                schemaBuilder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
            } else if (fieldValue instanceof Boolean) {
                schemaBuilder.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            } else {
                schemaBuilder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
            }
        }

        return schemaBuilder.build();
    }
}