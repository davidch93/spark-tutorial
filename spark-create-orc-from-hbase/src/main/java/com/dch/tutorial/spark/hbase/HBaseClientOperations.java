package com.dch.tutorial.spark.hbase;

import com.dch.tutorial.spark.model.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic HBase client operations.
 *
 * @author David.Christianto
 */
public class HBaseClientOperations {

    private static final Logger logger = LoggerFactory.getLogger(HBaseClientOperations.class);
    private static final String TABLE_NAME = "user";

    private final Connection connection;

    public HBaseClientOperations() {
        this.connection = getConnection();
    }

    /**
     * Method used to create HBase connection with specified configurations.
     *
     * @return {@link Connection} HBase connection. Default value is <code>null</code>.
     */
    private Connection getConnection() {
        try {
            Configuration config = HBaseConfiguration.create();
            return ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            logger.error("Error occurred while creating HBase connection!", e);
            throw new RuntimeException("Error occurred while creating HBase connection!");
        }
    }

    /**
     * Create the table with two column families.
     *
     * @throws IOException Error occurred when create the table.
     */
    public void createTable() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("name")).build())
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("contactInfo")).build())
                    .build();
            admin.createTable(tableDescriptor);
        }
    }

    /**
     * Add each person to the table. <br/>
     * Use the `name` column family for the name. <br/>
     * Use the `contactInfo` column family for the email
     *
     * @throws IOException Error occurred when put data into table.
     */
    public void put() throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            String[][] users = {{"1", "Marcel", "Haddad", "marcel@fabrikam.com"},
                    {"2", "Franklin", "Holtz", "franklin@contoso.com"},
                    {"3", "Dwayne", "McKeeleen", "dwayne@fabrikam.com"},
                    {"4", "Raynaldi", "Schroeder", "raynaldi@contoso.com"},
                    {"5", "Rosalie", "Burton", "rosalie@fabrikam.com"},
                    {"6", "Gabriela", "Ingram", "gabriela@contoso.com"}};

            for (String[] user : users) {
                Put put = new Put(Bytes.toBytes(user[0]))
                        .addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(user[1]))
                        .addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(user[2]))
                        .addColumn(Bytes.toBytes("contactInfo"), Bytes.toBytes("email"), Bytes.toBytes(user[3]));
                table.put(put);
            }
        }
    }

    /**
     * Method used to get all users.
     *
     * @return List of {@link User}
     * @throws IOException Error occurred when get the data.
     */
    public List<User> findAll() throws IOException {
        List<User> users = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result result : scanner) {
                User user = new User();
                user.setId(Long.parseLong(Bytes.toString(result.getRow())));
                user.setName(Bytes.toString(result.getValue(Bytes.toBytes("name"), Bytes.toBytes("first"))) + " "
                        + Bytes.toString(result.getValue(Bytes.toBytes("name"), Bytes.toBytes("last"))));
                user.setEmail(Bytes.toString(result.getValue(Bytes.toBytes("contactInfo"), Bytes.toBytes("email"))));
                users.add(user);
            }
        }
        return users;
    }

    /**
     * Method used to delete the specified table.
     *
     * @throws IOException Error occurred when delete the table.
     */
    public void deleteTable() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                admin.disableTable(TableName.valueOf(TABLE_NAME));
                admin.deleteTable(TableName.valueOf(TABLE_NAME));
            }
        }
    }
}
