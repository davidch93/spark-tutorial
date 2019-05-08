package com.dch.tutorial.spark.job.impl;

import com.dch.tutorial.spark.job.SparkStructureStreamerJob;
import com.dch.tutorial.spark.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.from_json;

/**
 * Example structure streaming job uses Spark that will use Debezium format for MySQL to aggregate records.
 * Note that the following Kafka params can't be set
 * https://spark.apache.org/docs/2.3.1/structured-streaming-kafka-integration.html#kafka-specific-configurations.
 *
 * @author david.christianto
 * @see com.dch.tutorial.spark.job.SparkStructureStreamerJob
 */
public class SampleSparkStructureStreamerJob implements SparkStructureStreamerJob {

    private static final String BOOTSTRAP_SERVERS = "172.18.11.94:9092,172.18.11.110:9092,172.18.12.110:9092";
    private static final String TOPIC_1 = "payment_invoiceable_mappers";
    private static final String TOPIC_2 = "payment_invoices";
    private static final StructType SCHEMA_1 = new StructType()
            .add("id", DataTypes.LongType)
            .add("invoice_id", DataTypes.LongType)
            .add("invoiceable_id", DataTypes.LongType)
            .add("invoiceable_type", DataTypes.StringType)
            .add("enum_invoiceable_type", DataTypes.IntegerType);
    private static final StructType SCHEMA_2 = new StructType()
            .add("id", DataTypes.LongType)
            .add("invoice_id", DataTypes.StringType)
            .add("state", DataTypes.StringType)
            .add("payment_method", DataTypes.StringType)
            .add("coded_amount", DataTypes.IntegerType)
            .add("paid_at", DataTypes.LongType)
            .add("created_at", DataTypes.LongType)
            .add("updated_at", DataTypes.LongType);

    @Override
    public void execute(SparkSession spark) throws Exception {
        Dataset<Row> dataset1 = readStream(spark, TOPIC_1, SCHEMA_1).as("pim");
        Dataset<Row> dataset2 = readStream(spark, TOPIC_2, SCHEMA_2).as("pi");
        /*
         * select pim.*, pi.state, pi.payment_method
         * from payment_invoiceable_mappers pim
         * inner join payment_invoices pi
         * where pim.invoice_id = pi.id
         */
        Dataset<Row> result = dataset1.join(dataset2, dataset1.col("pim.invoice_id").equalTo(dataset2.col("pi.id")))
                .select(column("pim.*"), column("pi.state"), column("pi.payment_method"));
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
            return payload.get("after").toString();
        }

        throw new RuntimeException(String.format("Unsupported operation type '%s'! Record: %s", operation, row));
    }
}
