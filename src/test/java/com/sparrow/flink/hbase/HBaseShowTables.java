package com.sparrow.flink.hbase;

import com.sparrow.stream.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;

import java.util.List;

public class HBaseShowTables {
    public static void main(String[] args) throws Exception {
        Connection connection = HBaseUtils.getConnection();
        Admin admin = connection.getAdmin();
        List<TableDescriptor> tableDescriptorList = admin.listTableDescriptors();
        System.out.println("tables " + tableDescriptorList.size() + "\n");
        tableDescriptorList.forEach(t -> {
            System.out.println(t.getTableName());
        });
        connection.close();
    }
}
