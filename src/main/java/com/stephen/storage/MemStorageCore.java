package com.stephen.storage;

import com.stephen.RaftState;
import eraftpb.Eraftpb;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class MemStorageCore {

    private RaftState raftState;

    // entries[i] has raft log position i+snapshot.get_metadata().index
    // 日志的位置+快照的索引
    private List<Eraftpb.Entry> entries;
    // Metadata of the last snapshot received.
    // 最后接收的快照
    private Eraftpb.SnapshotMetadata snapshotMetadata;
    // If it is true, the next snapshot will return a SnapshotTemporarilyUnavailable error.
    // 如果是true，下一个快照返回 SnapshotTemporarilyUnavailable 异常
    private boolean triggerSnapUnavailable;

    public MemStorageCore() {
        this.raftState = null;
        this.entries = new ArrayList<>();
        this.snapshotMetadata = Eraftpb.SnapshotMetadata.getDefaultInstance();
        this.triggerSnapUnavailable = false;
    }

    public void commitTo(long index) {
        assert (this.hasEntryAt(index));
        int diff = (int) (index - this.entries.stream().findFirst().map(Eraftpb.Entry::getIndex).orElse(0L));
        this.raftState.getHardState().toBuilder().setCommit(index);
        this.raftState.getHardState().toBuilder().setTerm(this.entries.get(diff).getTerm());
    }

    private boolean hasEntryAt(long index) {
        return !this.entries.isEmpty() && index >= this.firstIndex() && index <= this.lastIndex();
    }

    long firstIndex() {
        Eraftpb.Entry entry = this.entries.stream().findFirst().orElse(null);
        return entry == null ? this.snapshotMetadata.getIndex() + 1 : entry.getIndex();
    }

    long lastIndex() {
        Eraftpb.Entry entry = this.entries.stream().limit(this.entries.size() - 1).findFirst().orElse(null);
        return entry == null ? this.snapshotMetadata.getIndex() : entry.getIndex();
    }

    public Eraftpb.Snapshot snapshot() {
        return null;
    }
}
