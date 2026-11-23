package io.github.jzsyk77174.dlq;

import java.time.Clock;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * Order ingestion with an explicit failure path: records that do not parse go to a dead-letter
 * topic instead of failing the task.
 *
 * <p>Both sinks are exactly-once and the dead letters are a side output of the classifier, so a
 * checkpoint holds an order record together with its dead letter or neither of them.
 */
public final class DlqRouterJob {

    static final OutputTag<PoisonRecord> DEAD_LETTERS =
            new OutputTag<>("dead-letters", TypeInformation.of(PoisonRecord.class));

    private static final long CHECKPOINT_INTERVAL_MS = Duration.ofSeconds(30).toMillis();
    private static final long MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = Duration.ofSeconds(10).toMillis();
    private static final String TRANSACTION_TIMEOUT_MS = "900000";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrap = params.getRequired("bootstrap");
        String sourceTopic = params.getRequired("source-topic");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(MIN_PAUSE_BETWEEN_CHECKPOINTS_MS);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(sourceTopic)
                .setGroupId(params.getRequired("group-id"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        params.has("reset-earliest")
                                ? org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST
                                : org.apache.kafka.clients.consumer.OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "orders-source");

        SingleOutputStreamOperator<String> valid = raw
                .process(new ClassifyRecord(DEAD_LETTERS, sourceTopic, Clock.systemUTC()))
                .returns(Types.STRING)
                .name("classify-record")
                .uid("classify-record");

        valid.sinkTo(sink(bootstrap, params.getRequired("sink-topic"), "dlq-router-valid-"))
                .name("orders-sink")
                .uid("orders-sink");

        valid.getSideOutput(DEAD_LETTERS)
                .map(PoisonRecord::toJson)
                .returns(Types.STRING)
                .name("dead-letter-json")
                .uid("dead-letter-json")
                .sinkTo(sink(bootstrap, params.getRequired("dlq-topic"), "dlq-router-dlq-"))
                .name("dead-letter-sink")
                .uid("dead-letter-sink");

        env.execute("order-dlq-router");
    }

    private static KafkaSink<String> sink(String bootstrap, String topic, String transactionalId) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionalId)
                .setKafkaProducerConfig(java.util.Map.of(
                        "transaction.timeout.ms", TRANSACTION_TIMEOUT_MS))
                .build();
    }

    private DlqRouterJob() {
    }
}
