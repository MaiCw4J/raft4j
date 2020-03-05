package com.stephen.storage;

import com.stephen.RaftState;
import com.stephen.Storage;
import com.stephen.exception.PanicException;
import com.stephen.exception.RaftErrorException;
import com.stephen.lang.Vec;
import eraftpb.Eraftpb;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Data
@Slf4j
public class MemStorageCore {

    private RaftState raftState;

    // entries[i] has raft log position i+snapshot.get_metadata().index
    // 日志的位置+快照的索引
    private Vec<Eraftpb.Entry> entries;
    // Metadata of the last snapshot received.
    // 最后接收的快照
    private Eraftpb.SnapshotMetadata snapshotMetadata;
    // If it is true, the next snapshot will return a SnapshotTemporarilyUnavailable error.
    // 如果是true，下一个快照返回 SnapshotTemporarilyUnavailable 异常
    private boolean triggerSnapUnavailable;

    public MemStorageCore() {
        this.raftState = null;
        this.entries = new Vec<>();
        this.snapshotMetadata = Eraftpb.SnapshotMetadata.getDefaultInstance();
        this.triggerSnapUnavailable = false;
    }

    public void commitTo(long index) {
        // 判断日志数组不为空并且index在存在日志
        assert (this.hasEntryAt(index));
        //求第一个日志index到 indexd的差值
        int diff = (int) (index - this.entries.first().getIndex());
        //设置当前提交的index
        this.raftState.getHardState().toBuilder().setCommit(index);
        //设置选举届数
        this.raftState.getHardState().toBuilder().setTerm(this.entries.get(diff).getTerm());
    }

    /**
     * 判断日志数组不为空并且 index在存在日志
     */
    private boolean hasEntryAt(long index) {
        return !this.entries.isEmpty() && index >= this.firstIndex() && index <= this.lastIndex();
    }

    public long firstIndex() {
        //如果不存在则获取快照的索引并+1
        Eraftpb.Entry entry = this.entries.stream().findFirst().orElse(null);
        return entry == null ? this.snapshotMetadata.getIndex() + 1 : entry.getIndex();
    }

    public long lastIndex() {
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
            throw new PanicException(log, "storage first index > snapshot index ");
        }
        //clone
        //设置最新的index和term,并清空日志
        this.snapshotMetadata = metadata.toBuilder().build();
        this.getRaftState()
                .setHardState(this.getRaftState()
                        .getHardState()
                        .toBuilder()
                        .setTerm(metadata.getTerm())
                        .setCommit(metadata.getIndex())
                        .build());
        this.entries.clear();

        //更新配置文件状态
        this.getRaftState().setConfState(metadata.getConfState());
    }

    public Eraftpb.Snapshot snapshot(long requestIndex) throws RaftErrorException {
        return null;
    }

    public Eraftpb.Snapshot snapshot() {
        return Eraftpb.Snapshot
                .newBuilder()
                .setMetadata(Eraftpb.SnapshotMetadata.newBuilder()
                        .setIndex(this.getRaftState().getHardState().getCommit())
                        .setTerm(this.getRaftState().getHardState().getTerm())
                        .setConfState(this.getRaftState().getConfState().toBuilder().build())
                        .build())
                .build();
    }

    public void compact(long compactIndex) {
        if (compactIndex <= this.firstIndex()) {
            return;
        }
        if (compactIndex > this.lastIndex()) {
            throw new PanicException(log, String.format("compact not received raft logs: %d, last index: %d", compactIndex, this.lastIndex()));
        }
        //裁剪多余的日志
        this.entries.stream()
                .findFirst()
                .ifPresent(entry -> {
                    long offset = compactIndex - entry.getIndex();
                    this.entries.drain(0, (int) offset);
                });
    }

    /**
     * Append the new entries to storage.
     * Panics if `ents` contains compacted entries, or there's a gap between `ents` and the last
     * received entry in the storage.
     * <p>
     * 追加新的条目
     * 如果第一个index> 新追加添加的index，异常
     * 如果最大index < 新追加添加的index，异常
     * 在存储中接受  entry
     */
    public void append(List<Eraftpb.Entry> ents) {
        if (ents.isEmpty()) {
            return;
        }
        if (this.firstIndex() > ents.get(0).getIndex()) {
            throw new PanicException(log, String.format(
                    "overwrite compacted raft logs, compacted: %d, append: %d",
                    this.firstIndex() - 1,
                    ents.get(0).getIndex()
            ));
        }
        if (this.lastIndex() + 1 < ents.get(0).getIndex()) {
            throw new PanicException(log, String.format(
                    "raft logs should be continuous, last index: %d, new appended: %d",
                    this.lastIndex(),
                    ents.get(0).getIndex()
            ));
        }
        // Remove all entries overwritten by `ents`.
        long diff = ents.get(0).getIndex() - this.firstIndex();
        this.entries.drain(0, (int) diff);
        this.entries.addAll(ents);
    }

    /**
     * Commit to `idx` and set configuration to the given states. Only used for tests.
     */
    public void commitToAndSetConfStates(long idx, Eraftpb.ConfState cs) {
        this.commitTo(idx);
        Optional.ofNullable(cs).ifPresent(c -> this.getRaftState().setConfState(c));
    }

    /// Trigger a SnapshotTemporarilyUnavailable error.
    public void triggerSnapUnavailable() {
        this.triggerSnapUnavailable = true;
    }
}
