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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class MemStorage implements Storage {

    private MemStorageCore core;
    private ReentrantReadWriteLock rwl;

    public MemStorage() {
        this.core = new MemStorageCore();
        this.rwl = new ReentrantReadWriteLock();
    }

    /**
     * Create a new `MemStorage` with a given `Config`. The given `Config` will be used to
     * initialize the storage.
     * <p>
     * 根据配置初始化一个存储桶
     */
    public MemStorage(Eraftpb.ConfState confState) {
        this();
        this.initializeWithConfState(confState);
    }

    /**
     * Initialize a `MemStorage` with a given `Config`.
     */
    public void initializeWithConfState(Eraftpb.ConfState confState) {
        if (!this.initialState().initialized()) {
            throw new PanicException();
        }
        wl(core -> {
            core.setSnapshotMetadata(core.getSnapshotMetadata()
                    .toBuilder()
                    .setIndex(1)
                    .setTerm(1)
                    .build());
            RaftState raftState = core.getRaftState();
            raftState.setHardState(raftState.getHardState()
                    .setCommit(1)
                    .setTerm(1));
            raftState.setConfState(confState.toBuilder());
        });
    }

    /**
     * 写锁
     */
    public void wl(Consumer<MemStorageCore> consumer) {
        this.rwl.writeLock().lock();
        try {
            consumer.accept(this.core);
        } finally {
            this.rwl.writeLock().unlock();
        }
    }

    /**
     * 读锁
     */
    public <VALUE> VALUE rl(Function<MemStorageCore, VALUE> function) {
        this.rwl.readLock().lock();
        try {
            return function.apply(this.core);
        } finally {
            this.rwl.readLock().unlock();
        }
    }


    /**
     * Raft初始化时调用
     * RaftState包含  HardState（index，term） 、ConfState（当前所有节点）
     */
    @Override
    public RaftState initialState() {
        return this.rl(core -> {
            RaftState raftState = core.getRaftState();
            return new RaftState(raftState.getHardState().clone() , raftState.getConfState().clone());
        });
    }

    /**
     * 返回range（log，high）的日志条目，maxSize为最大条目数
     */
    @Override
    public Vec<Eraftpb.Entry> entries(long low, long high, Long maxSize) throws RaftErrorException {
        this.rwl.readLock().lock();
        try {
            if (low < this.core.firstIndex()) {
                throw new RaftErrorException(RaftError.Storage_Compacted);
            }
            if (high > this.core.lastIndex() + 1) {
                throw new PanicException(log, "index out of bound (last: {}, high: {})", this.core.lastIndex() + 1, high);
            }
            long offset = this.core.getEntries().get(0).getIndex();
            long lo = (low - offset);
            long hi = (high - offset);
            Vec<Eraftpb.Entry> entries = this.core.getEntries().stream()
                    .skip(lo)
                    .limit(hi - lo)
                    .collect(Collectors.toCollection(Vec::new));
            $.limitSize(entries, maxSize);
            return entries;
        } finally {
            this.rwl.readLock().unlock();
        }
    }


    /**
     * Implements the Storage trait.
     */
    @Override
    public long term(long idx) throws RaftErrorException {
        this.rwl.readLock().lock();
        try {
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
        } finally {
            this.rwl.readLock().unlock();
        }
    }

    @Override
    public long firstIndex() throws RaftErrorException {
        return rl(MemStorageCore::firstIndex);
    }

    @Override
    public long lastIndex() throws RaftErrorException {
        return rl(MemStorageCore::lastIndex);
    }

    /**
     * 返回最新的快照
     */
    @Override
    public Eraftpb.Snapshot snapshot(long requestIndex) throws RaftErrorException {
        AtomicBoolean flag = new AtomicBoolean(false);
        wl(core -> {
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
        });
        if (flag.get()) {
            throw new RaftErrorException(RaftError.Storage_SnapshotTemporarilyUnavailable);
        }
        return this.core.snapshot();
    }
}
