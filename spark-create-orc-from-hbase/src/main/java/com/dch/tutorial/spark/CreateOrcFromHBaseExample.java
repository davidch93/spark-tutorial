package com.dch.tutorial.spark;

import com.dch.tutorial.spark.hbase.HBaseClientOperations;
import com.dch.tutorial.spark.model.User;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.List;

/**
 * Example create ORC from HBase file using Spark.
 *
 * @author David.Christianto
 */
public class CreateOrcFromHBaseExample {

    private static final String HDFS_PATH = "hdfs://localhost:54310";
    private static final String ORC_WAREHOUSE_PATH = HDFS_PATH + "/user/hive/warehouse/";

    public static void main(String... args) throws IOException {
        // HBase
        HBaseClientOperations clientOperations = new HBaseClientOperations();
        clientOperations.createTable();
        clientOperations.put();
        List<User> users = clientOperations.findAll();
        clientOperations.deleteTable();

        // Spark
        SparkSession sparkSession = SparkSession.builder()
                .master("local[1]")
                .appName("CreateOrcFromHBaseExample")
                .getOrCreate();
        Dataset<Row> dataset = sparkSession.createDataFrame(users, User.class);
        dataset.write().mode(SaveMode.Append).format("orc").save(ORC_WAREHOUSE_PATH + "user_orc");
    }
}
