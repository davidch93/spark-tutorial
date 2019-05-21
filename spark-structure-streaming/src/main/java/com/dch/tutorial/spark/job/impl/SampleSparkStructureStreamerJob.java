package com.dch.tutorial.spark.job.impl;

import com.dch.tutorial.spark.job.SparkStructureStreamerJob;
import com.dch.tutorial.spark.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * Example structure streaming job uses Spark that will use Debezium format for MySQL to aggregate records.
 * Note that the following Kafka params can't be set
 * https://spark.apache.org/docs/2.3.1/structured-streaming-kafka-integration.html#kafka-specific-configurations.
 *
 * @author david.christianto
 * @see com.dch.tutorial.spark.job.SparkStructureStreamerJob
 */
public class SampleSparkStructureStreamerJob implements SparkStructureStreamerJob {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_1 = "remote_transactions";
    private static final String TOPIC_2 = "payment_invoiceable_mappers";
    private static final String TOPIC_3 = "payment_invoices";
    private static final StructType SCHEMA_1 = new StructType()
            .add("id", DataTypes.LongType)
            .add("amount", DataTypes.LongType)
            .add("paid_at", DataTypes.LongType)
            .add("remote_type", DataTypes.StringType)
            .add("timestamp", DataTypes.TimestampType);
    private static final StructType SCHEMA_2 = new StructType()
            .add("id", DataTypes.LongType)
            .add("invoice_id", DataTypes.LongType)
            .add("invoiceable_id", DataTypes.LongType)
            .add("invoiceable_type", DataTypes.StringType)
            .add("timestamp", DataTypes.TimestampType);
    private static final StructType SCHEMA_3 = new StructType()
            .add("id", DataTypes.LongType)
            .add("service_fee", DataTypes.LongType)
            .add("uniq_code", DataTypes.LongType)
            .add("timestamp", DataTypes.TimestampType);

    @Override
    public void execute(SparkSession spark) throws Exception {
        Dataset<Row> dataset1 = readStream(spark, TOPIC_1, SCHEMA_1).as("rt")
                .withWatermark("timestamp", "10 seconds")
                .dropDuplicates(new String[]{"id", "timestamp"});
        Dataset<Row> dataset2 = readStream(spark, TOPIC_2, SCHEMA_2).as("pim")
                .withWatermark("timestamp", "10 seconds")
                .dropDuplicates(new String[]{"id", "timestamp"});
        Dataset<Row> dataset3 = readStream(spark, TOPIC_3, SCHEMA_3).as("pi")
                .withWatermark("timestamp", "10 seconds")
                .dropDuplicates(new String[]{"id", "timestamp"});
        /*
         * select rt.*, pim.invoice_id, pi.service_fee, pi.uniq_code
         * from remote_transactions rt
         * inner join payment_invoiceable_mappers pim
         *   on pim.invoiceable_id = rt.id AND
         *     pim.invoiceable_type = 'Remote::Transaction' AND
         *     rt.timestamp >= pim.timestamp - interval 10 seconds AND
         *     rt.timestamp <= pim.timestamp + interval 3 weeks
         * inner join payment_invoices pi
         *   on pi.id = pim.invoice_id AND
         *     pi.timestamp >= pim.timestamp - interval 10 seconds AND
         *     pi.timestamp <= pim.timestamp + interval 3 weeks
         */
        Dataset<Row> result = dataset1
                .join(dataset2, expr("pim.invoiceable_id = rt.id AND " +
                        "pim.invoiceable_type = 'Remote::Transaction' AND " +
                        "rt.timestamp >= pim.timestamp - interval 10 seconds AND " +
                        "rt.timestamp <= pim.timestamp + interval 3 weeks"))
                .join(dataset3, expr("pi.id = pim.invoice_id AND " +
                        "pi.timestamp >= pim.timestamp - interval 10 seconds AND " +
                        "pi.timestamp <= pim.timestamp + interval 3 weeks"))
                .select(column("rt.*"), column("pim.invoice_id"), column("pi.service_fee"), column("pi.uniq_code"))
                .drop(column("rt.timestamp"));
        StreamingQuery query = result.writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .start();
        query.awaitTermination();
    }

    /**
     * Method used to create read stream from Kafka and load it to specific result.
     *
     * @param spark  {@link SparkSession}
     * @param topic  Kafka topic.
     * @param schema {@link StructType} Schema.
     * @return {@link Dataset} Result of data.
     */
    private Dataset<Row> readStream(SparkSession spark, String topic, StructType schema) {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", topic)
                .load()
                .select(column("value").cast(DataTypes.StringType))
                .filter(this::isNotTombStoneFlag)
                .map((MapFunction<Row, String>) this::transform, Encoders.STRING())
                .select(from_json(column("value"), schema).as("data"))
                .select(column("data.*"));
    }

    /**
     * Method used to check if the raw data is tombstone flag or not.
     *
     * @param row The record.
     * @return <code>true</code> if the raw data isn't tombstone flag and vice versa.
     */
    private boolean isNotTombStoneFlag(Row row) {
        JsonNode value = JsonUtil.toJsonNode(row.getAs("value"));
        return !value.get("schema").isNull() && !value.get("payload").isNull();
    }

    /**
     * Method used to transform {@link Row} into specific result.
     *
     * @param row The record.
     * @return String json.
     */
    private String transform(Row row) {
        JsonNode payload = JsonUtil.toJsonNode(row.getAs("value")).get("payload");
        String operation = payload.get("op").asText();
        if (operation.equalsIgnoreCase("r") || operation.equalsIgnoreCase("c") ||
                operation.equalsIgnoreCase("u")) {
            JsonNode body = payload.get("after");
            ((ObjectNode) body).put("timestamp", payload.get("ts_ms").asLong() / 1000L);
            return body.toString();
        }

        throw new RuntimeException(String.format("Unsupported operation type '%s'! Record: %s", operation, row));
    }
}
