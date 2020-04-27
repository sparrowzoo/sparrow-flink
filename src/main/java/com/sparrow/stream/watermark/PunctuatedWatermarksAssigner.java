package com.sparrow.stream.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class PunctuatedWatermarksAssigner implements AssignerWithPunctuatedWatermarks<WatermarkElement> {
    @Override
    public long extractTimestamp(WatermarkElement element, long previousElementTimestamp) {
        return element.getTimestamp();
    }

    @Override
    public Watermark checkAndGetNextWatermark(WatermarkElement lastElement, long extractedTimestamp) {
        Watermark watermark = lastElement.getTimestamp() != null ? new Watermark(extractedTimestamp) : null;
        System.out.println("checkAndGetNextWatermark THREAD-ID" + Thread.currentThread().getId() + "-watermark" + DateFormatUtils.format(watermark.getTimestamp(), DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()));
        return watermark;
    }
}