package com.sparrow.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class TableCSVDemo {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        final Schema schema = new Schema()
                .field("word", DataTypes.STRING())
                .field("word_count", DataTypes.INT());


        tEnv.connect(new FileSystem().path("D:\\workspace\\sparrow\\sparrow-flink\\src\\files\\word-count.csv"))
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("word_count");


        tEnv.connect(new FileSystem().path("D:\\workspace\\sparrow\\sparrow-flink\\src\\files\\word-count"))
                .withFormat(new Csv())
                .withSchema(schema)
                .createTemporaryTable("word_count_result");

        Table count = tEnv.from("word_count").where("LIKE(word, 'F%')").select("word,word_count");



        count.insertInto("word_count_result");
        tEnv.execute("job");
    }
}
