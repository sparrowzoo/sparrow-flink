package com.sparrow.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class MysqlConnectorDemo {
    public static void main(String[] args) throws Exception {
        // create a TableEnvironment for specific planner batch or streaming
        TableEnvironment tEnv = TableCreator.createBatchTableWithConfig();

        //org.apache.flink.table.api.TableException: BatchTableSink or OutputFormatTableSink required to emit batch Table.
        //tEnv=TableCreator.createBatchTableEnvWithExecution();
        //Exception in thread "main" org.apache.flink.table.api.SqlParserException: SQL parse failed. Encountered "count )" at line 1, column 17.

        final Schema schema = new Schema()
                .field("word", DataTypes.STRING())
                .field("word_count", DataTypes.INT());

        tEnv.connect(new FileSystem().path("D:\\workspace\\sparrow\\sparrow-flink\\src\\files\\word-count.csv"))
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("word_count");


        tEnv.sqlUpdate("CREATE TABLE word_count_result (`word` STRING, word_count INT) WITH (\n" +
                "  'connector.type' = 'jdbc', \n" +
                "  'connector.url' = 'jdbc:mysql://127.0.0.1/sparrow?serverTimezone=UTC&autoReconnect=true&failOverReadOnly=false&useUnicode=true&characterEncoding=utf-8',\n" +
                "  'connector.table' = 'word_count',\n" +
                "  'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "  'connector.username' = 'root',\n" +
                "  'connector.password' = '11111111'\n" +
                "  )");

        Table count = tEnv.sqlQuery(
                "SELECT word,SUM(word_count) AS word_count FROM word_count WHERE word like 'F%' GROUP BY word"
        );
        count.insertInto("word_count_result");
        tEnv.execute("job");
    }
}
