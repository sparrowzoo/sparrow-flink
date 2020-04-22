package com.sparrow.flink;

import com.sparrow.stream.utils.HBaseUtils;
import com.sparrow.stream.utils.UnitTimeUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class CompanyClickTest {
    public static void main(String[] args) throws Exception {
        Connection connection = HBaseUtils.getConnection();
        Admin admin = connection.getAdmin();
//        List<String> tables = HBaseUtils.showTables(admin);
//        for (String table : tables) {
//            System.out.println(table);
//        }
        TableName tableName = TableName.valueOf("company_click");
        ColumnFamilyDescriptor familyDesc = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf")).build();
        Table table = connection.getTable(tableName);
        String rowKey = null;
        String countCol = "count";
        long current = System.currentTimeMillis();
        for (long i = 0; i < 10; i++) {
            rowKey = 1 + "-" + (UnitTimeUtils.getReverseIntegralMillis(current + Duration.ofMinutes(10 * i).toMillis(), 10, TimeUnit.MINUTES));
            System.out.println(rowKey);
            //插入或更新数据
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(familyDesc.getName(), Bytes.toBytes(countCol), Bytes.toBytes(i));
            table.put(put);
        }

        //读取指定行数据
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        String rowResult = Bytes.toString(result.getRow());
        Integer count = Bytes.toInt(result.getValue(familyDesc.getName(), Bytes.toBytes(countCol)));
        System.out.println(rowResult + "count: " + count);

        //读取所有数据
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator<Result> it = resultScanner.iterator();
        while (it.hasNext()) {
            Result rs = it.next();
            String row = Bytes.toString(rs.getRow());
            Long ts = Long.MAX_VALUE - Long.valueOf(row.substring(2));
            String rowTime = DateFormatUtils.format(ts, "yyyy-MM-dd HH:mm:ss");
            System.out.println(rowTime + "-" + Bytes.toInt(rs.getValue(familyDesc.getName(), Bytes.toBytes(countCol))));
        }
        connection.close();
    }
}
