package com.stephen;

import com.google.protobuf.ByteString;
import com.stephen.exception.RaftError;
import com.stephen.exception.RaftErrorException;
import com.stephen.lang.Vec;
import com.stephen.raft.Ready;
import com.stephen.raft.SoftState;
import eraftpb.Eraftpb;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

import static com.stephen.constanst.Globals.INVALID_ID;

/**
 * RawNode is a thread-unsafe Node.
 * The methods of this struct correspond to the methods of Node and are described
 * more fully there.
 */
@Slf4j
@Data
@AllArgsConstructor
public class RawNode {

    private Raft raft;

    private Eraftpb.HardState prevHs;

    private SoftState prevSs;

    public RawNode(Config config, Storage store) {
        assert !(config.getId() == 0);
        Raft r = new Raft();
        RawNode rn = new RawNode(r, Eraftpb.HardState.getDefaultInstance(), new SoftState());
        rn.setPrevHs(rn.getRaft().hardState());
        rn.setPrevSs(rn.getRaft().softState());
        log.info("RawNode created with id {}.", rn.getRaft().getId());
    }

    public static RawNode withDefaultLogger(Config c, Storage store) {
        return new RawNode(c, store);
    }

    public void commitReady(Ready rd) {
        if (rd.getSs() != null) {
            this.prevSs = rd.getSs();
        }
        if (rd.getHs() != null && rd.getHs() == Eraftpb.HardState.getDefaultInstance()) {
            this.prevHs = rd.getHs();
        }
        if (!rd.getEntries().isEmpty()) {
            Eraftpb.Entry e = rd.getEntries().last();
            this.raft.getRaftLog().stableTo(e.getIndex(), e.getTerm());
        }
        if (rd.getSnapshot() != Eraftpb.Snapshot.getDefaultInstance()) {
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
        this.raft.step(Eraftpb.Message.getDefaultInstance()
                .toBuilder()
                .setMsgType(Eraftpb.MessageType.MsgHup)
                .build());
    }

    /**
     * Propose proposes data be appended to the raft log.
     */
    public void propose(ByteString context, ByteString data) throws RaftErrorException {
        this.raft.step(Eraftpb.Message.getDefaultInstance()
                .toBuilder()
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .setFrom(this.raft.getId())
                .addEntries(Eraftpb.Entry.getDefaultInstance()
                        .toBuilder()
                        .setData(data)
                        .setContext(context))
                .build());
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
        this.raft.step(Eraftpb.Message.getDefaultInstance()
                .toBuilder()
                .setMsgType(Eraftpb.MessageType.MsgPropose)
                .addEntries(Eraftpb.Entry.getDefaultInstance()
                        .toBuilder()
                        .setEntryType(Eraftpb.EntryType.EntryConfChange)
                        .setData(cc.toByteString())
                        .setContext(context))
                .build());
    }

    /**
     * Takes the conf change and applies it.
     */
    public Eraftpb.ConfState applyConfChange(Eraftpb.ConfChange cc) {
        if (cc.getNodeId() == INVALID_ID) {
            return Eraftpb.ConfState.getDefaultInstance()
                    .toBuilder()
                    .addAllVoters(new HashSet<>(this.raft.getPrs().voterIds()))
                    .addAllLearners(new HashSet<>(this.raft.getPrs().learnerIds()))
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
            this.raft.step(m);
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
}
