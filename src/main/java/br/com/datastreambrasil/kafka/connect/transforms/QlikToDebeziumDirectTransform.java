package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class QlikToDebeziumDirectTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Schema PAYLOAD_SCHEMA = SchemaBuilder.struct()
            .name("payload")
            .field("before", SchemaBuilder.struct().optional().build())
            .field("after", SchemaBuilder.struct().optional().build())
            .field("op", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

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
        Struct payloadStruct = new Struct(PAYLOAD_SCHEMA);

        // Copia toda a estrutura beforeData para before
        if (qlikMessage.containsKey("beforeData")) {
            payloadStruct.put("before", qlikMessage.get("beforeData"));
        }

        // Copia toda a estrutura data para after
        if (qlikMessage.containsKey("data")) {
            payloadStruct.put("after", qlikMessage.get("data"));
        }

        // Copia a operação
        if (qlikMessage.containsKey("headers")) {
            Map<String, Object> headers = (Map<String, Object>) qlikMessage.get("headers");
            Object operation = headers.get("operation");

            String op = "c";
            if (operation != null) {
                if ("UPDATE".equals(operation.toString())) {
                    op = "u";
                }
                if ("DELETE".equals(operation.toString())) {
                    op = "d";
                }
            }

            payloadStruct.put("op", op);
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                PAYLOAD_SCHEMA,
                payloadStruct,
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
}
