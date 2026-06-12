package com.snowflake.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.core.datastream.source.JdbcSource;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.RowResultExtractor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class SnowflakeToKafkaAsDatastream {

    private static final String SF_URL        = "jdbc:snowflake://.snowflakecomputing.com/";
    private static final String SF_USER       = "";
    private static final String SF_PASSWORD   = "";
    private static final String SF_QUERY      =
            "SELECT C_CUSTOMER_SK FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER LIMIT 10000";

    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_TOPIC   = "customer-sk";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String[] fieldNames = new String[]{"C_CUSTOMER_SK"};
        TypeInformation<?>[] fieldTypes = new TypeInformation[]{Types.LONG};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);

        JdbcSource<Row> jdbcSource = JdbcSource.<Row>builder()
                .setDriverName("net.snowflake.client.jdbc.SnowflakeDriver")
                .setDBUrl(SF_URL)
                .setUsername(SF_USER)
                .setPassword(SF_PASSWORD)
                .setSql(SF_QUERY)
                .setResultExtractor(new RowResultExtractor())
                .setTypeInformation(rowTypeInfo)
                .build();

        DataStream<Row> snowflakeStream = env.fromSource(
                jdbcSource,
                WatermarkStrategy.noWatermarks(),
                "SnowflakeSource"
        );

        DataStream<String> stringStream = snowflakeStream.map(row -> {
            Object val = row.getField(0);
            return val != null ? val.toString() : "";
        });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(KAFKA_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        stringStream.sinkTo(sink);

        env.execute();
    }
}