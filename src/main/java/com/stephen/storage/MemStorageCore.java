package com.stephen.storage;

import com.stephen.RaftState;
import com.stephen.exception.PanicException;
import eraftpb.Eraftpb;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
        // 判断日志数组不为空并且index在存在日志
        assert (this.hasEntryAt(index));
        //求第一个日志index到 indexd的差值
        int diff = (int) (index - this.entries.stream().findFirst().map(Eraftpb.Entry::getIndex).orElse(0L));
        //设置当前提交的index
        this.raftState.getHardState().toBuilder().setCommit(index);
        //设置选举届数
        this.raftState.getHardState().toBuilder().setTerm(this.entries.stream().skip(diff - 1).findFirst().map(Eraftpb.Entry::getTerm).orElse(0L));
    }

    /**
     * 判断日志数组不为空并且 index在存在日志
     */
    private boolean hasEntryAt(long index) {
        return !this.entries.isEmpty() && index >= this.firstIndex() && index <= this.lastIndex();
    }

    private long firstIndex() {
        //如果不存在则获取快照的索引并+1
        Eraftpb.Entry entry = this.entries.stream().findFirst().orElse(null);
        return entry == null ? this.snapshotMetadata.getIndex() + 1 : entry.getIndex();
    }

    private long lastIndex() {
        //如果不存在则获取快照的索引
        Eraftpb.Entry entry = this.entries.stream().skip(this.entries.size() - 1).findFirst().orElse(null);
        return entry == null ? this.snapshotMetadata.getIndex() : entry.getIndex();
    }

    /**
     * Overwrites the contents of this Storage object with those of the given snapshot.
     * Panics if the snapshot index is less than the storage's first index.
     * 用给定快照覆盖 ，如果快照的index小于 storage的第一个index则异常
     */
    public void applySnapshot(Eraftpb.Snapshot snapshot) {
        Eraftpb.SnapshotMetadata metadata = snapshot.getMetadata();
        long index = metadata.getIndex();
        if (this.firstIndex() > index) {
            throw new PanicException("storage first index > snapshot index ");
        }
        //clone
        //设置最新的index和term,并清空日志
        this.snapshotMetadata = metadata.toBuilder().clone().build();
        this.getRaftState().getHardState().toBuilder().setTerm(metadata.getTerm());
        this.getRaftState().getHardState().toBuilder().setCommit(metadata.getIndex());
        this.entries.clear();

        //更新配置文件状态
        this.getRaftState().setConfState(metadata.getConfState());
    }

    private Eraftpb.Snapshot snapshot() {
        Eraftpb.Snapshot snapshot = Eraftpb.Snapshot.getDefaultInstance();
        Eraftpb.SnapshotMetadata metadata = snapshot.getMetadata();
        metadata.toBuilder().setIndex(this.getRaftState().getHardState().getCommit());
        metadata.toBuilder().setTerm(this.getRaftState().getHardState().getTerm());
        //clone
        metadata.toBuilder().setConfState(this.getRaftState().getConfState().toBuilder().clone().build());
        return snapshot;
    }
}
