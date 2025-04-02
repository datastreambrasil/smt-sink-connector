package br.com.datastreambrasil.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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

        Map<String, Object> payload = new HashMap<>();
        Map<String, Object> newMessage = new HashMap<>();
        newMessage.put("payload", payload);

        payload.put("before", new HashMap<String, Object>());
        if (qlikMessage.containsKey("beforeData")) {
            Map<String, Object> beforeData = (Map<String, Object>) qlikMessage.get("beforeData");
            if (beforeData != null) {
                payload.put("before", beforeData);
            }
        }

        payload.put("after", new HashMap<String, Object>());
        if (qlikMessage.containsKey("data") && qlikMessage.get("data") != null) {
            Map<String, Object> afterData = (Map<String, Object>) qlikMessage.get("data");
            if (afterData != null) {
                payload.put("after", afterData);
            }
        }

        String op = "c";
        if (qlikMessage.containsKey("headers")) {
            Map<String, Object> headers = (Map<String, Object>) qlikMessage.get("headers");
            if (headers != null && headers.containsKey("operation")) {
                Object operation = headers.get("operation");
                if (operation != null) {
                    if ("UPDATE".equals(operation.toString())) {
                        op = "u";
                    }
                    if ("DELETE".equals(operation.toString())) {
                        op = "d";
                    }
                }
            }
        }
        payload.put("op", op);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                null,
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

}