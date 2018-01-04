package com.dch.tutorial.spark.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Basic HBase client operations.
 *
 * @author David.Christianto
 */
public class HBaseClientOperations {

    private static final String TABLE_NAME = "user";

    /**
     * Create the table with two column families.
     *
     * @param admin {@link Admin}
     * @throws IOException Error occurred when create the table.
     */
    public void createTable(Admin admin) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        tableDescriptor.addFamily(new HColumnDescriptor("name"));
        tableDescriptor.addFamily(new HColumnDescriptor("contactInfo"));
        admin.createTable(tableDescriptor);
    }

    /**
     * Add each person to the table. <br/>
     * Use the `name` column family for the name. <br/>
     * Use the `contactInfo` column family for the email
     *
     * @param table {@link Table}
     * @throws IOException Error occurred when put data into table.
     */
    public void put(Table table) throws IOException {
        String[][] users = {{"1", "Marcel", "Haddad", "marcel@fabrikam.com"},
                {"2", "Franklin", "Holtz", "franklin@contoso.com"},
                {"3", "Dwayne", "McKeeleen", "dwayne@fabrikam.com"},
                {"4", "Raynaldi", "Schroeder", "raynaldi@contoso.com"},
                {"5", "Rosalie", "Burton", "rosalie@fabrikam.com"},
                {"6", "Gabriela", "Ingram", "gabriela@contoso.com"}};

        for (int i = 0; i < users.length; i++) {
            Put put = new Put(Bytes.toBytes(users[i][0]));
            put.addImmutable(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(users[i][1]));
            put.addImmutable(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(users[i][2]));
            put.addImmutable(Bytes.toBytes("contactInfo"), Bytes.toBytes("email"), Bytes.toBytes(users[i][3]));
            table.put(put);
        }
    }

    /**
     * Method used to delete the specified table.
     *
     * @param admin     {@link Admin}
     * @param tableName Table name to delete.
     * @throws IOException
     */
    public void deleteTable(Admin admin, TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }
}
