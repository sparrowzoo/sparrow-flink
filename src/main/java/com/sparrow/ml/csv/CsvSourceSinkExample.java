package com.sparrow.ml.csv;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;

public class CsvSourceSinkExample {
    public static void main(String[] args) throws Exception {
        //String  url = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/movielens_ratings.csv";
        String url = "D:\\workspace\\sparrow\\open-source-integration-shell\\sparrow-flink\\src\\main\\java\\com\\sparrow\\ml\\movielens_ratings.csv";
        String schema = "userid bigint, movieid bigint, rating double, timestamp string";

        BatchOperator data = new CsvSourceBatchOp()
                .setFilePath(url).setSchemaStr(schema);

        data = data.groupBy("userid,movieid","userid,movieid,sum(rating)as rating");


        CsvSinkBatchOp allSink = new CsvSinkBatchOp()
                .setFilePath("d://ml/als/all.csv").setOverwriteSink(true);
        allSink.sinkFrom(data);


        SplitBatchOp spliter = new SplitBatchOp().setFraction(0.8);
        spliter.linkFrom(data);

        CsvSinkBatchOp train = new CsvSinkBatchOp()
                .setFilePath("d://ml/als/train.csv").setOverwriteSink(true);
        train.sinkFrom(spliter);

        CsvSinkBatchOp test = new CsvSinkBatchOp()
                .setFilePath("d://ml/als/test.csv").setOverwriteSink(true);
        test.sinkFrom(spliter.getSideOutput(0));

        BatchOperator.execute();

        /**
         * 里边有execute 方法
         *  @Override
         *     protected PrintBatchOp sinkFrom(BatchOperator in) {
         *
         *         this.setOutputTable(in.getOutputTable());
         *         if (null != this.getOutputTable()) {
         *             try {
         *                 logger.warn("ExecutionEnvironment execute");
         *                 List<Row> rows = DataSetConversionUtil.fromTable(getMLEnvironmentId(), this.getOutputTable()).collect();
         *                 logger.warn("result ------{}",rows.size());
         *                 batchPrintStream.println(TableUtil.formatTitle(this.getColNames()));
         *                 for (Row row : rows) {
         *                     batchPrintStream.println(TableUtil.formatRows(row));
         *                 }
         *             } catch (Exception ex) {
         *                 throw new RuntimeException(ex);
         *             }
         *         }
         *         return this;
         *     }
         */
        //data.print();
    }
}
