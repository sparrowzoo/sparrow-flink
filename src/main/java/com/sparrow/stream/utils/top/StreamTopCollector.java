package com.sparrow.stream.utils.top;

import java.util.LinkedList;

public abstract class StreamTopCollector<I extends KeyCountPair<K,V>,K, V extends Comparable<V>> {

    public StreamTopCollector(int top) {
        this.top = top;
    }

    private int top;

    public abstract I newInstance(KeyCountPair<K,V> keyCountPair);

    private boolean replace(LinkedList<I> list,KeyCountPair<K,V> current) {
        for (int i = 0; i < list.size(); i++) {
            KeyCountPair<K, V> keyCountPair = list.get(i);
            if (!keyCountPair.getKey().equals(current.getKey())) {
                continue;
            }
            //if key equals and less equals count
            if (current.getCount().compareTo(keyCountPair.getCount()) <= 0) {
                // abort
                return true;
            }
            list.remove(i);
            //must great current index
            this.insertInner(list, current, i);
            return true;
        }
        return false;
    }

    public void insert(LinkedList<I> list,KeyCountPair<K,V> current) {
        if (list.size() == 0) {
            list.add(this.newInstance(current));
            return;
        }
        if (this.replace(list, current)) {
            return;
        }
        KeyCountPair<K, V> first = list.getFirst();
        if (current.getCount().compareTo(first.getCount()) <= 0) {
            if (list.size() < top) {
                list.add(0, this.newInstance(current));
            }
            return;
        }
        this.insertInner(list, current, 0);
    }

    private void insertInner(LinkedList<I> list,KeyCountPair<K,V> current, int startIndex) {
        if (list.size() == 0) {
            list.add(this.newInstance(current));
            return;
        }
        KeyCountPair<K, V> last = list.getLast();
        if (current.getCount().compareTo(last.getCount()) > 0) {
            list.add(this.newInstance(current));
            if (list.size() > top) {
                list.removeFirst();
            }
            return;
        }

        //0,2,4,9
        for (int i = startIndex; i < list.size(); i++) {
            if (current.getCount().compareTo(list.get(i).getCount()) > 0) {
                continue;
            }
            list.add(i, this.newInstance(current));
            if (list.size() > top) {
                list.removeFirst();
            }
            return;
        }
    }
}
