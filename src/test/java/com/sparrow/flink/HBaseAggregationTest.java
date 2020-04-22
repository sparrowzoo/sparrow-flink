package com.sparrow.flink;

import com.sparrow.stream.utils.HBaseConfig;
import com.sparrow.stream.utils.HBaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;

public class HBaseAggregationTest {
    public static void main(String[] args) throws Throwable {
        HBaseConfig baseConfig = new HBaseConfig();
        baseConfig.setRpcTimeout(60L);
        baseConfig.setBatchWriteBuffer(2097152);
        baseConfig.setClientRetriesNumber(10);
        baseConfig.setClientPause(100L);
        baseConfig.setQuorum("dev04:2181,dev05:2181,dev06:2181");
        baseConfig.setParent("/hbase-cluster/hbase");

        LongColumnInterpreter columnInterpreter = new LongColumnInterpreter();
        AggregationClient aggregationClient = new AggregationClient(HBaseUtils.getConfig(baseConfig));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"),
                Bytes.toBytes("count"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(0));
        scan.setFilter(filter);
        Long max = aggregationClient.sum(TableName.valueOf("company_click"), columnInterpreter,
                scan);
        System.out.println(new BigDecimal(max.doubleValue()));
        aggregationClient.close();
    }
}
