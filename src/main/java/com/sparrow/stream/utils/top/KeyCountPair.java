package com.sparrow.stream.utils.top;

import java.io.Serializable;

public interface KeyCountPair<K, V extends Comparable<V>> extends Serializable {
    K getKey();

    V getCount();
}
