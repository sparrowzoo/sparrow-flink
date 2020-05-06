package com.sparrow.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/common.html#create-a-tableenvironment
 */
public class TableCreator {
    public static TableEnvironment createBatchTableEnvWithExecution() {
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        fbEnv.setParallelism(1);
        return BatchTableEnvironment.create(fbEnv);
    }

    public static StreamTableEnvironment createStreamTableEnv() {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        fsEnv.setParallelism(1);
        return StreamTableEnvironment.create(fsEnv, fsSettings);
    }


    public static TableEnvironment createBatchTableWithConfig() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        //https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/config.html
        // access flink configuration
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // set low-level key-value options
        //configuration.setString("table.exec.mini-batch.enabled", "true");
        //configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        //configuration.setString("table.exec.mini-batch.size", "5000");
        configuration.setInteger("table.exec.resource.default-parallelism", 1);
        return tableEnv;
    }
}
