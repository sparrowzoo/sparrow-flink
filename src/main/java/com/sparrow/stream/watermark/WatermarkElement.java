package com.sparrow.stream.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;

public class WatermarkElement {
    public WatermarkElement(String element, Long timestamp) {
        this.element = element;
        this.timestamp = timestamp;
    }

    public WatermarkElement() {
    }

    private String element;
    private Long timestamp;

    public String getElement() {
        return element;
    }

    public void setElement(String element) {
        this.element = element;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "WatermarkElement{" +
                "element='" + element + '\'' +
                ", timestamp=" + DateFormatUtils.format(timestamp,"yyyy-MM-dd HH:mm:ss") +
                '}';
    }
}
