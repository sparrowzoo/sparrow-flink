package com.sparrow.stream.utils;

import java.util.Comparator;
import java.util.Map;

public class DescendingHashValueComparator<K, V extends Comparable> implements Comparator<K> {
    private Map<K, V> hashMap;

    public DescendingHashValueComparator(Map<K, V> hashMap) {
        this.hashMap = hashMap;
    }

    /**
     * map.get(key)会导致取值为null 因为compare 方法未返回0
     * @param a
     * @param b
     * @return
     */
    public int compare(K a, K b) {
        int v = hashMap.get(b).compareTo(hashMap.get(a));
        if (v == 0) {
            return 1;
        }
        return v;
    }
}
