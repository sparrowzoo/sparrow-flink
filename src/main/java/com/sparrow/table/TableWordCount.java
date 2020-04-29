package com.sparrow.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class TableWordCount {

    private static StreamTableEnvironment createTableEnv() {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        return StreamTableEnvironment.create(fsEnv, fsSettings);
    }

    private static TableEnvironment createBatchTableEnv() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        return TableEnvironment.create(settings);
    }

    public static void main(String[] args) throws Exception {
        // create a TableEnvironment for specific planner batch or streaming
        TableEnvironment tEnv = createBatchTableEnv();
        final Schema schema = new Schema()
                .field("count", DataTypes.INT())
                .field("word", DataTypes.STRING());

        tEnv.connect(new FileSystem().path("D:\\workspace\\sparrow\\sparrow-flink\\src\\files\\word-count.csv"))
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("MySource1");
        tEnv.connect(new FileSystem().path("D:\\workspace\\sparrow\\sparrow-flink\\src\\files\\word-count-result"))
                .withFormat(new Csv())
                .withSchema(schema)
                .createTemporaryTable("MySink1");

        Table table1 = tEnv.from("MySource1").where("LIKE(word, 'F%')");
        table1.insertInto("MySink1");

        tEnv.execute("job");
        //String explanation = tEnv.explain(true);
        //System.out.println(explanation);
    }
}
