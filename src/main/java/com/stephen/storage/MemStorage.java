package com.stephen.storage;

import com.stephen.$;
import com.stephen.RaftState;
import com.stephen.Storage;
import com.stephen.exception.PanicException;
import com.stephen.exception.RaftError;
import com.stephen.exception.RaftErrorException;
import com.stephen.lang.Vec;
import eraftpb.Eraftpb;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

@Slf4j
public class MemStorage implements Storage {

    private AtomicReference<MemStorageCore> core;

    public MemStorage() {
        this.core = new AtomicReference<>();
    }

    /**
     * Create a new `MemStorage` with a given `Config`. The given `Config` will be used to
     * initialize the storage.
     * <p>
     * 根据配置初始化一个存储桶
     */
    public static <T extends Eraftpb.ConfState> MemStorage newWithConfState(T confState) {
        MemStorage storage = new MemStorage();
        storage.initializeWithConfState(confState);
        return storage;
    }

    /**
     * Initialize a `MemStorage` with a given `Config`.
     */
    public <T extends Eraftpb.ConfState> void initializeWithConfState(T confState) {
        assert (!this.initialState().initialized());
        wl(core -> {
            core.setSnapshotMetadata(core.getSnapshotMetadata()
                    .toBuilder()
                    .setIndex(1)
                    .setTerm(1)
                    .build());
            RaftState raftState = core.getRaftState();
            raftState.setHardState(raftState.getHardState()
                    .toBuilder()
                    .setCommit(1)
                    .setTerm(1)
                    .build());
            raftState.setConfState(confState);
            return core;
        });
    }

    /**
     * 写锁
     */
    public MemStorageCore wl(UnaryOperator<MemStorageCore> updateFunction) {
        return this.core.updateAndGet(updateFunction);
    }

    /**
     * 读锁
     */
    public MemStorageCore rl() {
        return this.core.get();
    }


    /**
     * Raft初始化时调用
     * RaftState包含  HardState（index，term） 、ConfState（当前所有节点）
     */
    @Override
    public RaftState initialState() {
        RaftState raftState = this.rl().getRaftState();
        return new RaftState(raftState.getHardState().toBuilder().build()
                , raftState.getConfState().toBuilder().build());
    }

    /**
     * 返回range（log，high）的日志条目，maxSize为最大条目数
     */
    @Override
    public List<Eraftpb.Entry> entries(long low, long high, Long maxSize) throws RaftErrorException {
        MemStorageCore core = rl();
        if (low < core.firstIndex()) {
            throw new RaftErrorException(RaftError.Storage_Compacted);
        }
        if (high > core.lastIndex() + 1) {
            throw new PanicException(log, "index out of bound (last: {}, high: {})", core.lastIndex() + 1, high);
        }
        long offset = core.getEntries().get(0).getIndex();
        long lo = (low - offset);
        long hi = (high - offset);
        Vec<Eraftpb.Entry> ents = core.getEntries().stream()
                .skip(lo)
                .limit(hi - lo)
                .collect(Collectors.toCollection(Vec::new));
        $.limitSize(ents, maxSize);
        return ents;
    }


    /**
     * Implements the Storage trait.
     */
    @Override
    public long term(long idx) throws RaftErrorException {
        MemStorageCore core = this.rl();
        if (idx == core.getSnapshotMetadata().getIndex()) {
            return core.getSnapshotMetadata().getTerm();
        }

        if (idx < core.firstIndex()) {
            throw new RaftErrorException(RaftError.Storage_Compacted);
        }

        long offset = core.getEntries().get(0).getIndex();
        assert !(idx >= offset);
        if (idx - offset >= core.getEntries().size()) {
            throw new RaftErrorException(RaftError.Storage_Unavailable);
        }
        return core.getEntries().get((int) (idx - offset)).getTerm();
    }

    @Override
    public long firstIndex() throws RaftErrorException {
        return rl().firstIndex();
    }

    @Override
    public long lastIndex() throws RaftErrorException {
        return rl().lastIndex();
    }

    /**
     * 返回最新的快照
     */
    @Override
    public Eraftpb.Snapshot snapshot(long requestIndex) throws RaftErrorException {
        AtomicBoolean flag = new AtomicBoolean(false);
        MemStorageCore wl = wl(core -> {
            if (core.isTriggerSnapUnavailable()) {
                core.setTriggerSnapUnavailable(false);
                flag.set(true);
            } else {
                Eraftpb.Snapshot snap = core.snapshot();
                if (snap.getMetadata().getIndex() < requestIndex) {
                    core.setSnapshotMetadata(snap.getMetadata()
                            .toBuilder()
                            .setIndex(requestIndex)
                            .build());
                }
            }
            return core;
        });
        if (flag.get()) {
            throw new RaftErrorException(RaftError.Storage_SnapshotTemporarilyUnavailable);
        }
        return wl.snapshot();
    }
}
