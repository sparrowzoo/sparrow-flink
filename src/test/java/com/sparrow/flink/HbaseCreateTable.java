package com.sparrow.flink;

import com.sparrow.stream.utils.HBaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseCreateTable {
    public static void main(String[] args) throws Exception {
        Connection connection = HBaseUtils.getConnection();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("company_click");
        ColumnFamilyDescriptor familyDesc = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf")).build();


        TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
                .setCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
                .setColumnFamily(familyDesc)
                //.setColumnFamilies()
                .build();
        admin.createTable(tableDesc);
    }
}
