package com.stephen.progress;

import com.stephen.exception.PanicException;

public class Inflights {

    // the starting index in the buffer
    private int start;
    // number of inflights in the buffer
    private int count;
    // ring buffer
    private long[] buffer;
    // initialCapacity
    private int capacity;

    public Inflights(int cap) {
        this.capacity = cap;
        this.buffer = new long[cap];
    }

    public boolean full() {
        return count == capacity;
    }

    // Adds an inflight into inflights
    public void add(long inflight) {

        if (this.full()) {
            throw new PanicException("cannot add into a full inflights");
        }

        int next = start + count;

        if (next >= capacity) {
            next -= capacity;
        }

        assert next <= buffer.length;

        buffer[next] = inflight;

        this.count++;

    }

    public void freeTo(long to) {
        if (count == 0 || to < buffer[start]) {
            return;
        }

        int i = 0;

        int idx = start;

        while (i < count) {
            if (to < buffer[idx]) {
                // found the first large inflight
                break;
            }

            // increase index and maybe rotate
            idx++;
            if (idx >= capacity) {
                idx -= capacity;
            }

            i++;
        }

        // free i inflights and set new start index
        count -= i;
        start = idx;
    }

    // Frees the first buffer entry.
    public void freeFirstOne() {
        long start = buffer[this.start];
        this.freeTo(start);
    }

    // Frees all inflights.
    public void reset() {
        count = start = 0;
    }

}
