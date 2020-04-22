package com.sparrow.stream.window;

import com.sparrow.stream.window.behivior.UserBehaviorBO;
import org.apache.flink.api.common.functions.AggregateFunction;

@Deprecated
public class AvgAggregation implements AggregateFunction<UserBehaviorBO, AverageAccumulator, Long> {
    @Override
    public AverageAccumulator createAccumulator() {
        System.out.println("init AverageAccumulator" + Thread.currentThread().getName());
        return new AverageAccumulator();
    }

    @Override
    public AverageAccumulator add(UserBehaviorBO userBehavior, AverageAccumulator averageAccumulator) {
        System.out.println("count" + averageAccumulator.getCount() + "- companyId:" + userBehavior);
        averageAccumulator.setSum(averageAccumulator.getSum() + userBehavior.getSkuId());
        averageAccumulator.setCount(averageAccumulator.getCount() + 1);
        return averageAccumulator;
    }

    @Override
    public Long getResult(AverageAccumulator averageAccumulator) {
        long avg = averageAccumulator.getSum() / averageAccumulator.getCount();
        System.out.println("avg " + avg);
        return avg;
    }

    @Override
    public AverageAccumulator merge(AverageAccumulator averageAccumulator, AverageAccumulator acc1) {
        averageAccumulator.setSum(averageAccumulator.getSum() + acc1.getSum());
        averageAccumulator.setCount(averageAccumulator.getCount() + acc1.getCount());
        System.out.println("marge" + averageAccumulator);
        return averageAccumulator;
    }
}
