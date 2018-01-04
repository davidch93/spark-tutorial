package com.dch.tutorial.spark;

import com.dch.tutorial.spark.config.SparkConfig;
import com.dch.tutorial.spark.model.User;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Example create ORC from HDFS file using Spark.
 *
 * @author David.Christianto
 */
public class CreateOrcFromHdfsExample {

    public static final String HDFS_PATH = "hdfs://localhost:54310";
    public static final String TEXT_FILE_PATH = HDFS_PATH + "/tmp/user.txt";
    public static final String ORC_WAREHOUSE_PATH = HDFS_PATH + "/user/hive/warehouse/";

    public static void main(String... args) {
        SparkSession sparkSession = SparkConfig.getSession("local", "CreateOrcFromHdfsExample");

        @SuppressWarnings("serial")
        JavaRDD<User> users = sparkSession.read().textFile(TEXT_FILE_PATH).javaRDD().map(new Function<String, User>() {
            @Override
            public User call(String line) throws Exception {
                String[] parts = line.split(",");
                User user = new User();
                user.setId(Long.parseLong(parts[0].trim()));
                user.setName(parts[1].trim());
                user.setEmail(parts[2].trim());
                return user;
            }
        });
        System.out.println(users.collect().toString());

        Dataset<Row> dataset = sparkSession.createDataFrame(users, User.class);
        dataset.write().mode(SaveMode.Append).format("orc").save(ORC_WAREHOUSE_PATH + "user_orc");

        sparkSession.close();
    }
}
