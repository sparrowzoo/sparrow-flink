package com.sparrow.stream.utils;

import java.util.concurrent.TimeUnit;

public class UnitTimeUtils {

    public static long getIntegralMillis(long millis,int times, TimeUnit timeUnit) {
        long unitMillis = timeUnit.toMillis(times);
        return millis / unitMillis * unitMillis;
    }

    public static long getReverseIntegralMillis(long millis,int times, TimeUnit timeUnit) {
        return Long.MAX_VALUE - getIntegralMillis(millis,times, timeUnit);
    }

    public static long getIntegralMillis(int times, TimeUnit timeUnit) {
        return getIntegralMillis(System.currentTimeMillis(),times,timeUnit);
    }

    public static long getReverseIntegralMillis(int times, TimeUnit timeUnit) {
        return getReverseIntegralMillis(System.currentTimeMillis(),times,timeUnit);
    }
}
