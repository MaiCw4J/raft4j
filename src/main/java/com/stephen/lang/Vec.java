package com.stephen.lang;


import java.util.ArrayList;
import java.util.Collection;

public class Vec<E> extends ArrayList<E> {

    public Vec() {
        super();
    }

    public Vec(Collection<? extends E> c) {
        super(c);
    }

    public void truncate(int limit) {
        removeRange(limit, this.size());
    }

    public void truncate(long limit) {
        removeRange((int) limit, this.size());
    }

    public void drain(int fromIndex, int toIndex) {
        removeRange(fromIndex, toIndex);
    }

    public E first() {
        return get(0);
    }

    public E last() {
        return get(this.size() - 1);
    }
}
