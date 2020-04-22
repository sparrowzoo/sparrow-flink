package com.sparrow.stream.window;

import com.sparrow.stream.window.behivior.UserBehaviorBO;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Deprecated
public class IncreamentAggregation implements AggregateFunction<UserBehaviorBO, List<UserBehaviorBO>, List<UserBehaviorBO>> {

    @Override
    public List<UserBehaviorBO> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<UserBehaviorBO> add(UserBehaviorBO userBehavior, List<UserBehaviorBO> userBehaviors) {
        if (!userBehaviors.contains(userBehavior)) {
            System.out.println(userBehavior.getCompanyId());
            userBehaviors.add(0, userBehavior);
        }
        return userBehaviors;
    }

    @Override
    public List<UserBehaviorBO> getResult(List<UserBehaviorBO> userBehaviors) {
        return userBehaviors;
    }

    @Override
    public List<UserBehaviorBO> merge(List<UserBehaviorBO> userBehaviors, List<UserBehaviorBO> acc1) {
        for (UserBehaviorBO userBehavior : acc1) {
            if (!userBehaviors.contains(userBehavior)) {
                userBehaviors.add(userBehavior);
            }
        }
        Collections.sort(userBehaviors);
        return userBehaviors;
    }
}
