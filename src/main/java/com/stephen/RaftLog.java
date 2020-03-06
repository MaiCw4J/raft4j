package com.stephen;

import com.stephen.exception.PanicException;
import com.stephen.exception.RaftError;
import com.stephen.exception.RaftErrorException;
import eraftpb.Eraftpb;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Data
public class RaftLog {

    /// Contains all stable entries since the last snapshot.
    private Storage store;

    /// Contains all unstable entries and snapshot.
    /// they will be saved into storage.
    private Unstable unstable;

    /// The highest log position that is known to be in stable storage
    /// on a quorum of nodes.
    private long committed;

    /// The highest log position that the application has been instructed
    /// to apply to its state machine.
    ///
    /// Invariant: applied <= committed
    private long applied;

    /// Creates a new raft log with a given storage and tag.
    public RaftLog(Storage store) throws RaftErrorException {
        long firstIndex = store.firstIndex() - 1;
        this.committed = firstIndex;
        this.applied = firstIndex;
        this.unstable = new Unstable(store.lastIndex() + 1);
        this.store = store;
    }

    /// Grabs the term from the last entry.
    ///
    /// # Panics
    ///
    /// Panics if there are entries but the last term has been discarded.
    public long lastTerm() {
        try {
            return this.term(this.lastIndex());
        } catch (RaftErrorException e) {
            throw new PanicException(log, "unexpected error when getting the last term", e);
        }
    }

    /// For a given index, finds the term associated with it.
    public long term(long idx) throws RaftErrorException {
        // the valid term range is [index of dummy entry, last index]
        var dummyIdx = this.firstIndex() - 1;
        if (idx < dummyIdx || idx > this.lastIndex()) {
            return 0;
        }

        var maybeTerm = this.unstable.maybeTerm(idx);
        if (maybeTerm != null) {
            return maybeTerm;
        }

        try {
            return this.store.term(idx);
        } catch (RaftErrorException e) {
            switch (e.getError()) {
                case Storage_Compacted, Storage_Unavailable -> throw e;
                default -> throw new PanicException(log, "unexpected error", e);
            }
        }
    }

    /// Returns th first index in the store that is available via entries
    ///
    /// # Panics
    ///
    /// Panics if the store doesn't have a first index.
    public long firstIndex() {
        return Optional.ofNullable(this.unstable.maybeFirstIndex()).orElseGet(() -> {
            try {
                return this.store.firstIndex();
            } catch (RaftErrorException e) {
                throw new PanicException(e);
            }
        });
    }


    /// Returns the last index in the store that is available via entries.
    ///
    /// # Panics
    ///
    /// Panics if the store doesn't have a last index.
    public long lastIndex() {
        return Optional.ofNullable(this.unstable.maybeLastIndex()).orElseGet(() -> {
            try {
                return this.store.lastIndex();
            } catch (RaftErrorException e) {
                throw new PanicException(e);
            }
        });
    }

    /// Finds the index of the conflict.
    ///
    /// It returns the first index of conflicting entries between the existing
    /// entries and the given entries, if there are any.
    ///
    /// If there are no conflicting entries, and the existing entries contain
    /// all the given entries, zero will be returned.
    ///
    /// If there are no conflicting entries, but the given entries contains new
    /// entries, the index of the first new entry will be returned.
    ///
    /// An entry is considered to be conflicting if it has the same index but
    /// a different term.
    ///
    /// The first entry MUST have an index equal to the argument 'from'.
    /// The index of the given entries MUST be continuously increasing.
    public long findConflict(List<Eraftpb.Entry> entries) {
        for (Eraftpb.Entry entry : entries) {
            if (this.matchTerm(entry.getIndex(), entry.getTerm())) {
                continue;
            }
            var idx = entry.getIndex();
            if (idx <= this.lastIndex()) {
                long termNow;
                try {
                    termNow = this.term(idx);
                } catch (RaftErrorException e) {
                    termNow = 0;
                }
                log.info("found conflict at index {}, existing term {}, conflicting term {}", idx, termNow, entry.getTerm());
            }
            return idx;
        }

        return 0;
    }

    /// Answers the question: Does this index belong to this term?
    public boolean matchTerm(long idx, long term) {
        try {
            return this.term(idx) == term;
        } catch (RaftErrorException e) {
            return false;
        }
    }

    public Long maybeAppend(long idx, long term, long committed, List<Eraftpb.Entry> entries) {
        if (this.matchTerm(idx, term)) {
            var conflictIdx = this.findConflict(entries);
            if (conflictIdx != 0 && conflictIdx <= this.committed) {
                throw new PanicException(log, "entry {} conflict with committed entry {}", conflictIdx, this.committed);
            } else {
                var start = (int) (conflictIdx - idx + 1);
                this.append(entries.subList(start, entries.size()));
            }

            var lastNewIndex = idx + entries.size();
            this.commitTo(Math.min(committed, lastNewIndex));
            return lastNewIndex;
        }
        return null;
    }

    /// Sets the last committed value to the passed in value.
    ///
    /// # Panics
    ///
    /// Panics if the index goes past the last index.
    public void commitTo(long committed) {
        if (this.committed >= committed) {
            // never decrease commit
            return;
        }

        var lastIndex = this.lastIndex();
        if (lastIndex < committed) {
            throw new PanicException(log, "to_commit {} is out of range [last_index {}]", committed, lastIndex);
        }

        this.committed = committed;
    }

    /// Attempts to set the stable up to a given index.
    public void stableTo(long idx, long term) {
        this.unstable.stableTo(idx, term);
    }

    /// Snaps the unstable up to a current index.
    public void stableTo(long idx) {
        this.unstable.stableSnapTo(idx);
    }

    /// Appends a set of entries to the unstable list.
    public long append(List<Eraftpb.Entry> entries) {
        if (entries == null || entries.isEmpty()) {
            return this.lastIndex();
        }

        var after = entries.get(0).getIndex() - 1;
        if (after < this.committed) {
            throw new PanicException(log, "after {} is out of range [committed {}]", after, this.committed);
        }

        this.unstable.truncateAndAppend(entries);
        return this.lastIndex();
    }

    /// Returns slice of entries that are not committed.
    public List<Eraftpb.Entry> unstableEntries() {
        var entries = this.unstable.getEntries();
        if (entries.isEmpty()) {
            return null;
        }
        return entries;
    }

    /// Returns the current snapshot
    public Eraftpb.Snapshot snapshot(long requestIndex) throws RaftErrorException {
        var snapshot = this.unstable.getSnapshot();
        if (snapshot != null && snapshot.getMetadata().getIndex() > requestIndex) {
            return snapshot;
        }
        return this.store.snapshot(requestIndex);
    }

    /// Returns entries starting from a particular index and not exceeding a bytesize.
    public List<Eraftpb.Entry> entries(long idx, long maxSize) throws RaftErrorException {
        var last = this.lastIndex();
        if (idx > last) {
            return List.of();
        }
        return this.slice(idx, last + 1, maxSize);
    }

    /// Grabs a slice of entries from the raft. Unlike a rust slice pointer, these are
    /// returned by value. The result is truncated to the max_size in bytes.
    public List<Eraftpb.Entry> slice(long low, long high, Long maxSize) throws RaftErrorException {
        this.mustCheckOutOfBounds(low, high);

        if (low == high) {
            return List.of();
        }

        List<Eraftpb.Entry> entries = new ArrayList<>();
        var offset = this.unstable.getOffset();
        if (low < offset) {
            var unstableHigh = Math.min(high, offset);

            try {
                entries = this.store.entries(low, unstableHigh, maxSize);
                if (entries.size() < unstableHigh - low) {
                    return entries;
                }
            } catch (RaftErrorException e) {
                switch (e.getError()) {
                    case Storage_Compacted -> throw e;
                    case Storage_Unavailable -> throw new PanicException(log, "entries[{}:{}] is unavailable from storage", low, unstableHigh);
                    default -> throw new PanicException(log, e);
                }
            }
        }

        if (high > offset) {
            var unstableEntries = this.unstable.slice(Math.max(low, offset), high);
            if ($.isNotEmpty(unstableEntries)) {
                entries.addAll(unstableEntries);
            }
        }

        var limit = $.limitSize(entries, maxSize);
        if (limit != null) {
            return entries.subList(0, limit.intValue());
        } else {
            return entries;
        }
    }

    private void mustCheckOutOfBounds(long low, long high) throws RaftErrorException {
        if (low > high) {
            throw new PanicException(log, "invalid slice {} > {}", low, high);
        }

        var firstIndex = this.firstIndex();
        if (low < firstIndex) {
            throw new RaftErrorException(RaftError.Storage_Compacted);
        }

        var lastIndex = this.lastIndex();
        if (low < firstIndex || high > lastIndex + 1) {
            throw new PanicException(log, "slice[{},{}] out of bound[{},{}]", low, high, firstIndex, lastIndex);
        }
    }

    /// Attempts to commit the index and term and returns whether it did.
    public boolean maybeCommit(long maxIndex, long term) {
        long maxIndexTerm;
        try {
            maxIndexTerm = this.term(maxIndex);
        } catch (RaftErrorException e) {
            maxIndexTerm = 0;
        }

        if (maxIndex > this.committed && maxIndexTerm == term) {
            if (log.isDebugEnabled()) {
                log.debug("committing index {}", maxIndex);
            }
            this.commitTo(maxIndex);
            return true;
        } else {
            return false;
        }
    }

    /// Determines if the given (lastIndex,term) log is more up-to-date
    /// by comparing the index and term of the last entry in the existing logs.
    /// If the logs have last entry with different terms, then the log with the
    /// later term is more up-to-date. If the logs end with the same term, then
    /// whichever log has the larger last_index is more up-to-date. If the logs are
    /// the same, the given log is up-to-date.
    public boolean isUpToDate(long lastIndex, long term) {
        var lastTerm = this.lastTerm();
        return term > lastTerm || (term == lastTerm && lastIndex >= this.lastIndex());
    }

    /// Restores the current log from a snapshot.
    public void restore(Eraftpb.Snapshot snapshot) {
        var meta = snapshot.getMetadata();
        log.info("log unstable starts to restore snapshot [index: {}, term: {}]",
                meta.getIndex(),
                meta.getTerm());
        this.committed = meta.getIndex();
        this.unstable.restore(snapshot);
    }

}
