package com.sparrow.stream.utils;

import java.util.Comparator;
import java.util.Map;

public class AscendingHashValueComparator<K, V extends Comparable> implements Comparator<K> {
    private Map<K, V> hashMap;

    public AscendingHashValueComparator(Map<K, V> hashMap) {
        this.hashMap = hashMap;
    }

    public int compare(K a, K b) {
        int v = hashMap.get(a).compareTo(hashMap.get(b));
        if (v == 0) {
            return -1;
        }
        return v;
    }
}
