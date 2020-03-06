package com.stephen.raft;

import com.stephen.Raft;
import com.stephen.ReadState;
import com.stephen.lang.Vec;
import eraftpb.Eraftpb;
import lombok.Data;

import java.util.Optional;
import java.util.stream.Collectors;

@Data
public class Ready {

    private SoftState ss;

    private Eraftpb.HardState hs;

    private Vec<ReadState> readStates;

    private Vec<Eraftpb.Entry> entries;

    private Eraftpb.Snapshot snapshot;

    /**
     * CommittedEntries specifies entries to be committed to a
     * store/state-machine. These have previously been committed to stable
     */
    private Vec<Eraftpb.Entry> committedEntries;

    /**
     * Messages specifies outbound messages to be sent AFTER Entries are
     * committed to stable storage.
     * If it contains a MsgSnap message, the application MUST report back to raft
     * when the snapshot has been received or has failed by calling ReportSnapshot.
     */
    private Vec<Eraftpb.Message> messages;

    private boolean mustSync;

    public Ready() {
        this.entries = new Vec<>();
        this.committedEntries = new Vec<>();
        this.ss = new SoftState();
        this.hs = Eraftpb.HardState.getDefaultInstance();
        this.readStates = new Vec<>();
        this.snapshot = Eraftpb.Snapshot.getDefaultInstance();
        this.messages = new Vec<>();
        this.mustSync = false;
    }

    public Ready(Raft raft, SoftState prevSs, Eraftpb.HardState prevHs, Long sinceIdx) {
        this();
        this.entries = raft.getRaftLog()
                .unstableEntries()
                .stream()
                .collect(Collectors.toCollection(Vec::new));
        if (!raft.getMsgs().isEmpty()) {
            this.setMessages(new Vec<>(raft.getMsgs()));
            raft.getMsgs().clear();
        }
        this.committedEntries = Optional.ofNullable(
                Optional.ofNullable(sinceIdx)
                        .map(idx -> raft.getRaftLog().nextEntriesSince(idx))
                        .orElse(raft.getRaftLog().nextEntries()))
                .orElseGet(Vec::new);
        SoftState ss = raft.softState();
        if (ss != prevSs) {
            this.setSs(ss);
        }
        Eraftpb.HardState hs = raft.hardState();
        if (hs != prevHs) {
            if (hs.getVote() != prevHs.getVote() || hs.getTerm() != prevHs.getTerm()) {
                this.mustSync = true;
            }
            this.hs = hs;
        }
        if (raft.getRaftLog().getUnstable().getSnapshot() != null) {
            this.snapshot = raft.getRaftLog().getUnstable().getSnapshot().toBuilder().build();
        }
        if (!raft.getReadStates().isEmpty()) {
            this.setReadStates(new Vec<>(raft.getReadStates()));
        }
    }
    
}
