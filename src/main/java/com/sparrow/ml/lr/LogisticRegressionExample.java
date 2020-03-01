package com.sparrow.ml.lr;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.classification.LogisticRegressionModel;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.List;

public class LogisticRegressionExample {
    public static void main(String[] args) throws Exception {
        String url = "D:\\workspace\\sparrow\\open-source-integration-shell\\sparrow-flink\\src\\main\\java\\com\\sparrow\\ml\\csv\\movielens_ratings.csv";
        String schema = "f0 bigint, f1 bigint, label double, weight double";

        BatchOperator data = new CsvSourceBatchOp()
                .setFilePath(url).setSchemaStr(schema);

        LogisticRegression lr = new LogisticRegression()
                .setFeatureCols("f0", "f1")
                .setLabelCol("label")
                .setPredictionCol("pred")
                .setPredictionDetailCol("pred_detail")
                .setWeightCol("weight")
                .setStandardization(false)
                .setWithIntercept(true);
        LogisticRegressionModel model = lr.fit(data);
        List<Row> list = BatchOperator.fromTable(model.getModelData()).collect();
        for (Row row : list) {
            System.out.println(row.toString());
        }
        model.transform(data).print();
    }
}
