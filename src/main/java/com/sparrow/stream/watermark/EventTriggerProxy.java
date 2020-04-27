package com.sparrow.stream.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class EventTriggerProxy extends Trigger<WatermarkElement, TimeWindow> {
    private Trigger trigger = EventTimeTrigger.create();

    @Override
    public TriggerResult onElement(WatermarkElement element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("onElement THREAD-ID" + Thread.currentThread().getId() + "element:" + element + " window:" + DateFormatUtils.format(window.getStart(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()) + "-" + DateFormatUtils.format(window.getEnd(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()) + " watermark:" + DateFormatUtils.format(ctx.getCurrentWatermark(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()));
        return trigger.onElement(element, timestamp, window, ctx);
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return trigger.onProcessingTime(time, window, ctx);
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("onEventTime element time:" + DateFormatUtils.format(time, DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()) + " window:" + DateFormatUtils.format(window.getStart(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()) + "-" + DateFormatUtils.format(window.getEnd(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()));
        return trigger.onEventTime(time, window, ctx);
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        trigger.clear(window, ctx);
    }
}
