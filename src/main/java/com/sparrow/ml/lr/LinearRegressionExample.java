package com.sparrow.ml.lr;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.regression.LinearRegression;
import com.alibaba.alink.pipeline.regression.LinearRegressionModel;

public class LinearRegressionExample {
    public static void main(String[] args) throws Exception {
        String url = "D:\\workspace\\sparrow\\open-source-integration-shell\\sparrow-flink\\src\\main\\java\\com\\sparrow\\ml\\csv\\movielens_ratings.csv";
        String schema = "user_id bigint, movie_id bigint, label double, timestamp string";

        BatchOperator data = new CsvSourceBatchOp()
                .setFilePath(url).setSchemaStr(schema);

        LinearRegression lr = new LinearRegression()
                .setFeatureCols("user_id", "movie_id")
                .setLabelCol("label")
                .setPredictionCol("pred")
                .setWeightCol("weight");
                //.setReservedCols("")
        LinearRegressionModel model = lr.fit(data);
        model.transform(data).print();
    }
}
