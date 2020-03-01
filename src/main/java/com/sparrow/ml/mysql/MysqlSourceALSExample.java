package com.sparrow.ml.mysql;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.recommendation.AlsPredictBatchOp;
import com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.MySqlSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MySqlSourceBatchOp;

public class MysqlSourceALSExample {
    public static void main(String[] args) throws Exception {
        MySqlSourceBatchOp data = new MySqlSourceBatchOp()
                .setIp("localhost")
                .setPort("3306")
                .setDbName("sparrow")
                .setUsername("root")
                .setPassword("11111111")
                .setInputTableName("rating");


        SplitBatchOp spliter = new SplitBatchOp().setFraction(0.8);
        spliter.linkFrom(data);

        //训练数据
        BatchOperator trainData = spliter;
        //预数据
        BatchOperator testData = spliter.getSideOutput(0);

        AlsTrainBatchOp als = new AlsTrainBatchOp()
                .setUserCol("user_id")
                .setItemCol("movie_id")
                .setRateCol("rating")
                .setNumIter(10).setRank(10).setLambda(0.01);


        BatchOperator model = als.linkFrom(trainData);
        AlsPredictBatchOp predictor = new AlsPredictBatchOp()
                .setUserCol("user_id")
                .setItemCol("movie_id")
                .setPredictionCol("prediction_result");

        BatchOperator preditionResult = predictor.linkFrom(model, data).select("user_id,movie_id,rating, prediction_result").orderBy("user_id", 10);

        MySqlSinkBatchOp preditionSink = new MySqlSinkBatchOp()
                .setIp("localhost")
                .setPort("3306")
                .setDbName("sparrow")
                .setUsername("root")
                .setPassword("11111111")
                .setOutputTableName("rating_predition");
        preditionSink.sinkFrom(preditionResult);


        BatchOperator userFactors = model.select("user_id,factors").where("user_id is not null");

        MySqlSinkBatchOp userFactorsSink = new MySqlSinkBatchOp()
                .setIp("localhost")
                .setPort("3306")
                .setDbName("sparrow")
                .setUsername("root")
                .setPassword("11111111")
                .setOutputTableName("user_factors");
        userFactorsSink.sinkFrom(userFactors);


        MySqlSinkBatchOp itemFactorsSink = new MySqlSinkBatchOp()
                .setIp("localhost")
                .setPort("3306")
                .setDbName("sparrow")
                .setUsername("root")
                .setPassword("11111111")
                .setOutputTableName("item_factors");
        BatchOperator itemFactors = model.select("movie_id,factors").where("movie_id is not null");
        itemFactorsSink.sinkFrom(itemFactors);
        BatchOperator.execute();
    }
}
