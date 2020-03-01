package com.sparrow.stream.window.count;

import com.sparrow.stream.window.behivior.UserBehaviorBO;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import java.util.Iterator;

public class DeduplicateFilter extends RichFilterFunction<UserBehaviorBO> {
    private MapState<Integer, Long> duplicateMap;

    @Override
    public boolean filter(UserBehaviorBO userBehaviorBO) throws Exception {
        if (null == userBehaviorBO) {
            return false;
        }

        Integer id = userBehaviorBO.getSkuId();
        if (duplicateMap.contains(id)) {
            return false;
        } else {
            duplicateMap.put(id, System.currentTimeMillis()/1000);
            Iterator<Integer> it=duplicateMap.keys().iterator();
            while (it.hasNext()){
                int key=it.next();
                System.out.println("state key :"+key+" ttl:"+(System.currentTimeMillis()/1000-duplicateMap.get(key)));
            }
            return true;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Integer, Long> descriptor = new MapStateDescriptor<Integer, Long>("duplicate", TypeInformation.of(Integer.class),
                TypeInformation.of(Long.class));

        System.out.println("open state");

        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(60))
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupIncrementally(5, false).build();
        descriptor.enableTimeToLive(stateTtlConfig);
        this.duplicateMap = this.getRuntimeContext().getMapState(descriptor);
    }
}
