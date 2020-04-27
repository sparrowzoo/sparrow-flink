package com.sparrow.stream.window.time;

import com.sparrow.stream.window.behivior.UserBehaviorBO;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Override public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
 * return ProcessingTimeTrigger.create();
 * }
 * @see SlidingProcessingTimeWindows
 */
public class TimeTriggerProxy extends Trigger<UserBehaviorBO, TimeWindow> {

    ProcessingTimeTrigger trigger = ProcessingTimeTrigger.create();


    /**
     * ctx.registerProcessingTimeTimer(window.maxTimestamp());
     * //自动fire
     * return TriggerResult.CONTINUE;
     */
    @Override
    public TriggerResult onElement(UserBehaviorBO element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println(" on element:" + element + " window:" + DateFormatUtils.format(window.getStart(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()) + "-" + DateFormatUtils.format(window.getEnd(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()) + " watermark:" + DateFormatUtils.format(ctx.getCurrentWatermark(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()));
        return trigger.onElement(element, timestamp, window, ctx);
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return this.trigger.onProcessingTime(time, window, ctx);
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return this.trigger.onEventTime(time, window, ctx);
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        trigger.clear(window, ctx);
    }
}
