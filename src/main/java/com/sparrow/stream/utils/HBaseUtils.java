package com.sparrow.stream.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtils {
    private static final String HBASE_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_PARENT = "zookeeper.znode.parent";
    private static final String HBASE_BATCH_WRITE_BUFFER = "hbase.batch.write.buffer";
    private static final String HBASE_RPC_TIMEOUT = "hbase.rpc.timeout";
    private static final String HBASE_CLIENT_RETRIES_NUMBER = "hbase.client.retries.number";
    private static final String HBASE_CLIENT_PAUSE = "hbase.client.pause";

    public static Configuration getConfig(HBaseConfig hbaseConfig) throws IOException {
        Configuration basicConfig = HBaseConfiguration.create();
        basicConfig.set(HBASE_RPC_TIMEOUT, String.valueOf(hbaseConfig.getRpcTimeout()));
        basicConfig.set(HBASE_BATCH_WRITE_BUFFER, String.valueOf(hbaseConfig.getBatchWriteBuffer()));
        basicConfig.set(HBASE_CLIENT_RETRIES_NUMBER, String.valueOf(hbaseConfig.getClientRetriesNumber()));
        basicConfig.set(HBASE_CLIENT_PAUSE, String.valueOf(hbaseConfig.getClientPause()));
        // 创建configuration
        Configuration conf = HBaseConfiguration.create(basicConfig);
        conf.set(HBASE_QUORUM, hbaseConfig.getQuorum());
        conf.set(HBASE_PARENT, hbaseConfig.getParent());
        return conf;
    }

    public static Connection getConnection(HBaseConfig hbaseConfig) throws IOException {
        Configuration basicConfig = getConfig(hbaseConfig);
        return ConnectionFactory.createConnection(basicConfig);
    }

    public static Connection getConnection() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.addResource("hbase_config.xml");
        return ConnectionFactory.createConnection(config);
    }

    public void close(Connection connection) throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    public static List<String> showTables(Admin admin) throws IOException {
        List<TableDescriptor> tableDescriptorList = admin.listTableDescriptors();
        List<String> tables = new ArrayList<>();
        tableDescriptorList.forEach(t -> {
            tables.add(t.getTableName().getNameAsString());
        });
        return tables;
    }
}
