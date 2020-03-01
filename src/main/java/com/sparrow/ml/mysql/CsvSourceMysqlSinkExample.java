package com.sparrow.ml.mysql;

import com.alibaba.alink.common.io.MySqlDB;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.MySqlSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;

public class CsvSourceMysqlSinkExample {
    public static void main(String[] args) throws Exception {
        //String  url = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/movielens_ratings.csv";
        String url = "D:\\workspace\\sparrow\\open-source-integration-shell\\sparrow-flink\\src\\main\\java\\com\\sparrow\\ml\\csv\\movielens_ratings.csv";
        String schema = "user_id bigint, movie_id bigint, rating double, timestamp string";

        BatchOperator data = new CsvSourceBatchOp()
                .setFilePath(url).setSchemaStr(schema);

        //String dbName, String ip, String port, String username, String password
        MySqlDB mySqlDB=new MySqlDB("sparrow","localhost","3306","root","11111111");
        mySqlDB.dropTable("rating");

        MySqlSinkBatchOp allSink = new MySqlSinkBatchOp()
                .setIp("localhost")
                .setPort("3306")
                .setDbName("sparrow")
                .setUsername("root")
                .setPassword("11111111")
                .setOutputTableName("rating");

        allSink.sinkFrom(data);
        BatchOperator.execute();
    }
}
