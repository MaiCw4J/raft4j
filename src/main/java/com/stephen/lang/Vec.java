package com.stephen.lang;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

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

    public Optional<E> first() {
        if (isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(get(0));
    }

    public Optional<E> last() {
        if (isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(get(this.size() - 1));
    }
}
