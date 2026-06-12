package com.snowflake.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.core.datastream.source.JdbcSource;
import org.apache.flink.connector.jdbc.core.datastream.source.reader.extractor.RowResultExtractor; // Required package
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SnowflakeToKafka {

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
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        tableEnv.createTemporaryView("SnowflakeSourceView", snowflakeStream);

        tableEnv.executeSql(
                "CREATE TABLE KafkaSink (" +
                        "  C_CUSTOMER_SK STRING" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = '" + KAFKA_TOPIC + "'," +
                        "  'properties.bootstrap.servers' = '" + KAFKA_BROKERS + "'," +
                        "  'format' = 'raw'" +
                        ")"
        );

        tableEnv.executeSql(
                "INSERT INTO KafkaSink " +
                        "SELECT CAST(C_CUSTOMER_SK AS STRING) " +
                        "FROM SnowflakeSourceView"
        ).await();
    }
}