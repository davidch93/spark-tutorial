package com.dch.tutorial.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.dch.tutorial.spark.config.HBaseConfig;
import com.dch.tutorial.spark.config.SparkConfig;
import com.dch.tutorial.spark.hbase.HBaseClientOperations;
import com.dch.tutorial.spark.model.User;

/**
 * Example create ORC from HBase file using Spark.
 * 
 * @author David.Christianto
 */
public class CreateOrcFromHBaseExample {

	private static final String HBASE_TABLE_NAME = "user";
	private static final byte[] FAMILY_NAME = Bytes.toBytes("name");
	private static final byte[] FAMILY_CONTACT_INFO = Bytes.toBytes("contactInfo");
	private static final byte[] QUALIFIER_FIRST = Bytes.toBytes("first");
	private static final byte[] QUALIFIER_LAST = Bytes.toBytes("last");
	private static final byte[] QUALIFIER_EMAIL = Bytes.toBytes("email");

	private static final String HDFS_PATH = "hdfs://localhost:54310";
	private static final String ORC_WAREHOUSE_PATH = HDFS_PATH + "/user/hive/warehouse/";

	public static void main(String... args) {
		try {
			// HBase
			HBaseClientOperations clientOperations = new HBaseClientOperations();
			Connection connection = HBaseConfig.getConnection();

			TableName tableName = TableName.valueOf(HBASE_TABLE_NAME);
			if (!connection.getAdmin().tableExists(tableName)) {
				clientOperations.createTable(connection.getAdmin());
				clientOperations.put(connection.getTable(tableName));
			}

			List<User> users = new ArrayList<>();
			Table table = connection.getTable(tableName);
			try (ResultScanner scanner = table.getScanner(new Scan())) {
				for (Result result : scanner) {
					User user = new User();
					user.setId(Long.parseLong(Bytes.toString(result.getRow())));
					user.setName(Bytes.toString(result.getValue(FAMILY_NAME, QUALIFIER_FIRST)) + " "
							+ Bytes.toString(result.getValue(FAMILY_NAME, QUALIFIER_LAST)));
					user.setEmail(Bytes.toString(result.getValue(FAMILY_CONTACT_INFO, QUALIFIER_EMAIL)));
					users.add(user);
				}
			}

			clientOperations.deleteTable(connection.getAdmin(), tableName);

			// Spark
			SparkSession sparkSession = SparkConfig.getSession("local", "CreateOrcFromHBaseExample");

			Dataset<Row> dataset = sparkSession.createDataFrame(users, User.class);
			dataset.write().mode(SaveMode.Append).format("orc").save(ORC_WAREHOUSE_PATH + "user_orc");

			sparkSession.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
