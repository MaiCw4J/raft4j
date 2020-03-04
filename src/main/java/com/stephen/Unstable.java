package com.stephen;

import com.stephen.exception.PanicException;
import eraftpb.Eraftpb;
import lombok.Data;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

@Data
public class Unstable {

    /// The incoming unstable snapshot, if any.
    private Eraftpb.Snapshot snapshot;

    /// All entries that have not yet been written to storage.
    private List<Eraftpb.Entry> entries;

    /// The offset from the vector index.
    private long offset;

    /// Creates a new log of unstable entries.
    public Unstable(long offset) {
        this.offset = offset;
        this.entries = new ArrayList<>();
    }

    /// Returns the index of the first possible entry in entries
    /// if it has a snapshot.
    public Long maybeFirstIndex() {
        if (this.snapshot == null) {
            return null;
        }
        return this.snapshot.getMetadata().getIndex();
    }

    /// Returns the last index if it has at least one unstable entry or snapshot.
    public Long maybeLastIndex() {
        if (this.entries.isEmpty()) {
            // if entries is empty can return first index in snapshot
            return maybeFirstIndex();
        } else {
            return this.offset + this.entries.size() - 1;
        }
    }

    /// Returns the term of the entry at index idx, if there is any.
    public Long maybeTerm(long idx) {

        if (idx < this.offset) {

            if (this.snapshot == null) {
                return null;
            }

            var meta = this.snapshot.getMetadata();
            if (idx == meta.getIndex()) {
                return meta.getTerm();
            } else {
                return null;
            }
        } else {
            var lastIndex = this.maybeLastIndex();

            if (lastIndex == null || idx > lastIndex) {
                return null;
            }

            return this.entries.get((int) (idx - this.offset)).getTerm();
        }
    }

    /// Moves the stable offset up to the index. Provided that the index
    /// is in the same election term.
    public void stableTo(long idx, long term) {
        var maybeTerm = this.maybeTerm(idx);
        if (maybeTerm == null) {
            return;
        }

        if (maybeTerm == term && idx >= this.offset) {
            var start = idx + 1 - this.offset;
            this.drain((int)start);
            this.offset = idx + 1;
        }
    }

    /// Removes the snapshot from self if the index of the snapshot matches
    public void stableSnapTo(long idx) {
        if (this.snapshot == null) {
            return;
        }

        if (idx == this.snapshot.getMetadata().getIndex()) {
            this.snapshot = null;
        }
    }

    /// From a given snapshot, restores the snapshot to self, but doesn't unpack.
    public void restore(@NonNull Eraftpb.Snapshot snapshot) {
        this.entries.clear();
        this.offset = snapshot.getMetadata().getIndex() + 1;
        this.snapshot = snapshot;
    }

    /// Append entries to unstable, truncate local block first if overlapped.
    public void truncateAndAppend(List<Eraftpb.Entry> entries) {
        var after = entries.get(0).getIndex();
        if (after == this.offset + this.entries.size()) {
            // after is the next index in the self.entries, append directly
            this.entries.addAll(entries);
        } else if (after <= this.offset) {
            // The log is being truncated to before our current offset
            // portion, so set the offset and replace the entries
            this.offset = after;
            this.entries.clear();
            this.entries.addAll(entries);
        } else {
            // truncate to after and copy to self.entries then append
            var offset = this.offset;
            this.mustCheckOutOfBounds(offset, after);
            this.truncate((int)(after - offset));
            this.entries.addAll(entries);
        }
    }

    /// Returns a slice of entries between the high and low.
    ///
    /// # Panics
    ///
    /// Panics if the `lo` or `hi` are out of bounds.
    /// Panics if `lo > hi`.
    public List<Eraftpb.Entry> slice(long low, long high) {
        this.mustCheckOutOfBounds(low, high);
        return this.entries.subList((int) (low - this.offset), (int) (high - this.offset));
    }

    /// Asserts the `hi` and `lo` values against each other and against the
    /// entries themselves.
    public void mustCheckOutOfBounds(long low, long high) {
        if (low > high) {
            throw new PanicException(String.format("invalid unstable.slice %d > %d", low, high));
        }

        var upper = this.offset + this.entries.size();

        if (low < this.offset || high > upper) {
            throw new PanicException(String.format("unstable.slice[%d, %d] out of bound[%d, %d]", low, high, this.offset, upper));
        }
    }

    private void drain(int index) {
        var entries = this.entries;
        this.entries = new ArrayList<>(entries.subList(index, entries.size()));
    }

    private void truncate(int index) {
        this.entries = new ArrayList<>(this.entries.subList(0, index));
    }


}
