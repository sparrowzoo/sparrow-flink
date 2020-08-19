package com.sparrow.stream.window.count;

import com.sparrow.stream.window.behivior.UserBehaviorBO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.List;

public class OperatorFunctionMapper implements MapFunction<UserBehaviorBO,UserBehaviorBO>, CheckpointedFunction {

    private transient ListState<UserBehaviorBO> checkpointedState;

    private List<UserBehaviorBO> bufferedElements;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (UserBehaviorBO element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<UserBehaviorBO> descriptor = new ListStateDescriptor<>("buffered-elements", TypeInformation.of(new TypeHint<UserBehaviorBO>() {
        }));
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {// isRestored：检查是否是失败后恢复
            for (UserBehaviorBO element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }


    @Override
    public UserBehaviorBO map(UserBehaviorBO userBehaviorBO) throws Exception {
        return null;
    }
}
