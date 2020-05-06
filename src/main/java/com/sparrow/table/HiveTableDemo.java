package com.sparrow.table;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveTableDemo {
    public static void main(String[] args) {
        String name = "myhive";
        String defaultDatabase = "mydatabase";
        String hiveConfDir = "src/main/resources/";
        String version = "2.3.4";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        TableEnvironment tableEnvironment = TableCreator.createBatchTableEnvWithExecution();

        tableEnvironment.registerCatalog("myhive", hive);
        // set the HiveCatalog as the current catalog of the session
        tableEnvironment.useCatalog("myhive");
        Table table = tableEnvironment.sqlQuery("show databases");
        table.printSchema();
    }
}
