package com.sparrow.flink.hbase;

import com.sparrow.stream.utils.HBaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

public class HbaseDropTable {
    public static void main(String[] args) throws Exception {
        Connection connection = HBaseUtils.getConnection();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("company_click");
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
}
