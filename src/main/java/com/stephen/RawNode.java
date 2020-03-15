package com.stephen;

import com.google.protobuf.ByteString;
import com.stephen.constanst.SnapshotStatus;
import com.stephen.exception.PanicException;
import com.stephen.exception.RaftError;
import com.stephen.exception.RaftErrorException;
import eraftpb.Eraftpb;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;

import static com.stephen.constanst.Globals.INVALID_ID;

/**
 * RawNode is a thread-unsafe Node.
 * The methods of this struct correspond to the methods of Node and are described
 * more fully there.
 */
@Slf4j
@Data
@AllArgsConstructor
public class RawNode<T extends Storage> {

    private Raft<T> raft;

    private Eraftpb.HardState prevHs;

    private SoftState prevSs;

    public RawNode(Config config, T store) throws RaftErrorException {
        if (config.getId() == 0) {
            throw new PanicException("config.id must not be zero");
        }

        this.raft = new Raft<>(config, store);
        this.prevHs = this.raft.hardState();
        this.prevSs = this.raft.softState();

        log.info("RawNode created with id {}.", this.raft.getId());
    }

    public static <T extends Storage> RawNode<T> withDefaultLogger(Config c, T store) throws RaftErrorException {
        return new RawNode<>(c, store);
    }

    public void commitReady(Ready rd) {
        if (rd.getSs() != null) {
            this.prevSs = rd.getSs();
        }

        var hh = rd.getHs();
        if (hh != null && !Objects.equals(rd.getHs(), Eraftpb.HardState.getDefaultInstance())) {
            this.prevHs = hh;
        }

        rd.getEntries().last()
                .ifPresent(last -> this.raft.getRaftLog().stableTo(last.getIndex(), last.getTerm()));

        if (Objects.equals(rd.getSnapshot(), Eraftpb.Snapshot.getDefaultInstance())) {
            this.raft.getRaftLog().stableSnapTo(rd.getSnapshot().getMetadata().getIndex());
        }
        if (!rd.getReadStates().isEmpty()) {
            this.raft.getReadStates().clear();
        }
    }

    public void commitApply(long applied) {
        this.raft.commitApply(applied);
    }

    public boolean tick() {
        return this.raft.tick();
    }

    /**
     * Campaign causes this RawNode to transition to candidate state.
     */
    public void campaign() throws RaftErrorException {
        this.raft.step(Eraftpb.Message.newBuilder().setMsgType(Eraftpb.MessageType.MsgHup));
    }

    /**
     * Propose proposes data be appended to the raft log.
     */
    public void propose(ByteString context, ByteString data) throws RaftErrorException {
        this.raft.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .setFrom(this.raft.getId())
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setData(data)
                        .setContext(context)));
    }

    /**
     * Broadcast heartbeats to all the followers.
     * If it's not leader, nothing will happen.
     */
    public void ping() {
        this.raft.ping();
    }

    /**
     * ProposeConfChange proposes a config change.
     */
    public void proposeConfChange(ByteString context, Eraftpb.ConfChange cc) throws RaftErrorException {
        this.raft.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.newBuilder()
                        .setEntryType(Eraftpb.EntryType.EntryConfChange)
                        .setData(cc.toByteString())
                        .setContext(context)));
    }

    /**
     * Takes the conf change and applies it.
     */
    public Eraftpb.ConfState applyConfChange(Eraftpb.ConfChange cc) throws RaftErrorException {
        if (cc.getNodeId() == INVALID_ID) {
            return Eraftpb.ConfState.newBuilder()
                    .addAllVoters(this.raft.getPrs().voterIds())
                    .addAllLearners(this.raft.getPrs().learnerIds())
                    .build();
        }
        long nid = cc.getNodeId();
        switch (cc.getChangeType()) {
            case AddNode -> this.raft.addNode(nid);
            case AddLearnerNode -> this.raft.addLearner(nid);
            case RemoveNode -> this.raft.removeNode(nid);
        }
        return this.raft.getPrs().getConfiguration().toConfState();
    }

    /**
     * Step advances the state machine using the given message.
     */
    public void step(Eraftpb.Message m) throws RaftErrorException {
        // ignore unexpected local messages receiving over network
        if (isLocalMsg(m.getMsgType())) {
            throw new RaftErrorException(RaftError.StepLocalMsg);
        }
        if (this.raft.getPrs().get(m.getFrom()) != null || !isResponseMsg(m.getMsgType())) {
            this.raft.step(m.toBuilder());
            return;
        }
        throw new RaftErrorException(RaftError.StepPeerNotFound);
    }

    private boolean isResponseMsg(Eraftpb.MessageType msgType) {
        return switch (msgType) {
            case MsgAppendResponse, MsgRequestVoteResponse, MsgHeartbeatResponse, MsgUnreachable, MsgRequestPreVoteResponse -> true;
            default -> false;
        };
    }

    private boolean isLocalMsg(Eraftpb.MessageType msgType) {
        return switch (msgType) {
            case MsgHup, MsgBeat, MsgUnreachable, MsgSnapStatus, MsgCheckQuorum -> true;
            default -> false;
        };
    }

    /**
     * Given an index, creates a new Ready value from that index.
     */
    public Ready readySince(long appliedIdx) {
        return new Ready(this.raft, this.prevSs, this.prevHs, appliedIdx);
    }

    /**
     * Ready returns the current point-in-time state of this RawNode.
     */
    public Ready ready() {
        return new Ready(this.raft, this.prevSs, this.prevHs, null);
    }

    /**
     * HasReady called when RawNode user need to check if any Ready pending.
     * Checking logic in this method should be consistent with Ready.containsUpdates()
     */
    public boolean hasReady() {
        return this.hasReadySince(null);
    }

    /**
     * Given an index, can determine if there is a ready state from that time.
     */
    public boolean hasReadySince(Long appliedIdx) {
        if (!this.raft.getMsgs().isEmpty() || this.raft.getRaftLog().unstableEntries() != null) {
            return true;
        }

        if (!this.raft.getReadStates().isEmpty()) {
            return true;
        }

        if (Optional.ofNullable(this.snap()).map(this::isEmptySnap).orElse(false)) {
            return true;
        }

        if (Optional.ofNullable(appliedIdx)
                .map(idx -> this.raft.getRaftLog().hasNextEntriesSince(idx))
                .orElseGet(() -> this.raft.getRaftLog().hasNextEntries())) {
            return true;
        }

        if (!Objects.equals(this.raft.softState(), this.prevSs)) {
            return true;
        }

        Eraftpb.HardState hs = this.raft.hardState();
        return !Objects.equals(hs, Eraftpb.HardState.getDefaultInstance()) && !Objects.equals(hs, this.prevHs);
    }

    public Eraftpb.Snapshot snap() {
        return this.raft.snap();
    }

    /**
     * For a given snapshot, determine if it's empty or not.
     */
    public boolean isEmptySnap(Eraftpb.Snapshot s) {
        return s.getMetadata().getIndex() == 0;
    }

    /**
     * Appends and commits the ready value.
     */
    public void advanceAppend(Ready rd) {
        this.commitReady(rd);
    }

    /**
     * Advance apply to the passed index.
     */
    public void advanceApply(long applied) {
        this.commitApply(applied);
    }

    /**
     * Status returns the current status of the given group.
     */
    public Status status() {
        return new Status(this.raft);
    }

    /**
     * Returns the current status of the given group.
     * It's borrows the internal progress set instead of copying.
     */
    public StatusRef statusRef() {
        return new StatusRef(this.raft);
    }

    /**
     * ReportUnreachable reports the given node is not reachable for the last send.
     */
    public void reportUnreachable(long id) throws RaftErrorException {
        this.raft.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgUnreachable)
                .setFrom(id));
    }

    /**
     * ReportSnapshot reports the status of the sent snapshot.
     */
    public void reportSnapshot(long id, SnapshotStatus status) throws RaftErrorException {
        boolean rej = status == SnapshotStatus.Failure;
        this.raft.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgSnapStatus)
                .setFrom(id)
                .setReject(rej));
    }

    /**
     * Request a snapshot from a leader.
     * The snapshot's index must be greater or equal to the request_index.
     */
    public void requestSnapshot(long requestIndex) throws RaftErrorException {
        this.raft.requestSnapshot(requestIndex);
    }

    /**
     * TransferLeader tries to transfer leadership to the given transferee.
     */
    public void transferLeader(long transferee) throws RaftErrorException {
        this.raft.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgTransferLeader)
                .setFrom(transferee));
    }

    /**
     * ReadIndex requests a read state. The read state will be set in ready.
     * Read State has a read index. Once the application advances further than the read
     * index, any linearizable read requests issued before the read request can be
     * processed safely. The read state will have the same rctx attached.
     */
    public void readIndex(ByteString rctx) throws RaftErrorException {
        this.raft.step(Eraftpb.Message.newBuilder()
                .setMsgType(Eraftpb.MessageType.MsgReadIndex)
                .addEntries(Eraftpb.Entry.newBuilder().setData(rctx)));
    }

    /**
     * Returns the store as an immutable reference.
     */
    public Storage store() {
        return this.raft.store();
    }


    /**
     * Returns the store as a mutable reference.
     */
    public Storage mutStore() {
        return this.raft.mutStore();
    }

    /**
     * Set whether skip broadcast empty commit messages at runtime.
     */
    public void skipBCastCommit(boolean skip) {
        this.raft.setSkipBCastCommit(skip);
    }

    public void setBatchAppend(boolean batchAppend) {
        this.raft.setBatchAppend(batchAppend);
    }

    /// Advance notifies the RawNode that the application has applied and saved progress in the
    /// last Ready results.
    public void advance(Ready ready) {
        this.advanceAppend(ready);
        var commitIdx = this.prevHs.getCommit();
        if (commitIdx != 0) {
            // In most cases, prevHardSt and rd.HardState will be the same
            // because when there are new entries to apply we just sent a
            // HardState with an updated Commit value. However, on initial
            // startup the two are different because we don't send a HardState
            // until something changes, but we do send any un-applied but
            // committed entries (and previously-committed entries may be
            // incorporated into the snapshot, even if rd.CommittedEntries is
            // empty). Therefore we mark all committed entries as applied
            // whether they were included in rd.HardState or not.
            this.advanceApply(commitIdx);
        }
    }


}
