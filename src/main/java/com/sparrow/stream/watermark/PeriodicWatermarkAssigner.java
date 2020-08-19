package com.sparrow.stream.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
 * if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
 * // if the watermark is already past the window fire immediately
 * return TriggerResult.FIRE;
 * } else {
 * ctx.registerEventTimeTimer(window.maxTimestamp());
 * return TriggerResult.CONTINUE;
 * }
 * }
 */
public class PeriodicWatermarkAssigner implements AssignerWithPeriodicWatermarks<WatermarkElement> {

    //每个partition 都会有一个watermark
    //触发逻辑会取所有分区中最小的 P62
    //https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/event_timestamps_watermarks.html
    private long currentMaxTimestamp = 0L;
    private final long maxOutOfOrderness = 5000L;   //这个控制失序已经延迟的度量,时间戳10秒以前的数据

    //事件驱动
    @Override
    public long extractTimestamp(WatermarkElement element, long previousElementTimestamp) {
        if (element == null) {
            return currentMaxTimestamp;
        }

        long timestamp = element.getTimestamp();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        System.out.println("get timestamp is " + DateFormatUtils.format(timestamp, "yyyy-MM-dd HH:mm:ss") + " currentMaxTimestamp " + DateFormatUtils.format(currentMaxTimestamp, "yyyy-MM-dd HH:mm:ss"));
        return timestamp;
    }

    //获取Watermark(周期性执行)
    @Override
    public Watermark getCurrentWatermark() {
        System.out.println(Thread.currentThread().getId() + "wall clock is " + System.currentTimeMillis() + " new watermark " + (currentMaxTimestamp - maxOutOfOrderness));
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}
