package com.stephen;

import com.stephen.exception.PanicException;
import com.stephen.exception.RaftErrorException;
import eraftpb.Eraftpb;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

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
                throw e.panic();
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
                throw e.panic();
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

    public void maybeAppend(long idx, long term, long committed, List<Eraftpb.Entry> entries) {
        if (this.matchTerm(idx, term)) {
            var conflictIdx = this.findConflict(entries);
            if (conflictIdx != 0 && conflictIdx <= this.committed) {
                throw new PanicException(log, "entry {} conflict with committed entry {}", conflictIdx, this.committed);
            } else {
                var start = (int) (conflictIdx - idx + 1);
                this.append(entries.subList(start, entries.size()));
//                et start = (conflict_idx - (idx + 1)) as usize;
//                self.append(&ents[start..]);
            }
        }
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

}
