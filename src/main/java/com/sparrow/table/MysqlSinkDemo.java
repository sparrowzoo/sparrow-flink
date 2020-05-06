package com.sparrow.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import java.sql.Types;

public class MysqlSinkDemo {
    public static void main(String[] args) throws Exception {
        // create a TableEnvironment for specific planner batch or streaming
        TableEnvironment tEnv = TableCreator.createBatchTableEnvWithExecution();

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

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1/sparrow?serverTimezone=UTC&autoReconnect=true&failOverReadOnly=false&useUnicode=true&characterEncoding=utf-8")
                .setUsername("root")
                .setPassword("11111111")
                .setQuery("INSERT INTO word_count_sink (word,word_count) VALUES (?,?)")
                .setParameterTypes(Types.VARCHAR, Types.INTEGER)
                .build();

        tEnv.registerTableSink(
                "word_count_sink",
                new String[]{"word", "word_count"},
                new TypeInformation[]{TypeInformation.of(String.class), TypeInformation.of(Integer.class)},
                sink);

        Table count = tEnv.sqlQuery(
                "SELECT word,SUM(word_count) AS word_count FROM word_count WHERE word like 'F%' GROUP BY word"
        );
        count.insertInto("word_count_sink");
        tEnv.execute("job");
    }
}
