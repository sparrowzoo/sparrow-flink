package com.sparrow.stream.window.count;

import com.sparrow.stream.window.behivior.UserBehaviorBO;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class CountTriggerProxy extends Trigger<UserBehaviorBO, Window> {
    private CountTrigger countTrigger;

    public CountTriggerProxy(int slide) {
        this.countTrigger = CountTrigger.of(slide);
    }

    @Override
    public TriggerResult onElement(UserBehaviorBO element, long timestamp, Window window, TriggerContext ctx) throws Exception {
        System.out.println("on element "+element.toString()+"-"+System.currentTimeMillis()+"-"+window.toString());
        return countTrigger.onElement(element, timestamp, window, ctx);
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
        return countTrigger.onProcessingTime(time, window, ctx);
    }

    @Override
    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
        return countTrigger.onEventTime(time, window, ctx);
    }

    @Override
    public void clear(Window window, TriggerContext ctx) throws Exception {
        countTrigger.clear(window, ctx);
    }
}
