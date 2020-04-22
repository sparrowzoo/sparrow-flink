package com.sparrow.flink.hbase;

import com.sparrow.stream.utils.HBaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;

public class HBaseTableDesc {
    public static void main(String[] args) throws Exception {
        Connection connection = HBaseUtils.getConnection();
        TableName tableName = TableName.valueOf("company_click");
        Table table = connection.getTable(tableName);
        TableDescriptor tableDescriptor = table.getDescriptor();
        System.out.println(tableDescriptor.getTableName().getNameAsString());
        ColumnFamilyDescriptor[] columnFamilyDescriptors = tableDescriptor.getColumnFamilies();
        for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilyDescriptors) {
            System.out.println(columnFamilyDescriptor.getNameAsString());
        }
    }
}

