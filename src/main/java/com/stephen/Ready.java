package com.stephen;

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

    public <T extends Storage> Ready(Raft<T> raft, SoftState prevSs, Eraftpb.HardState prevHs, Long sinceIdx) {
        this.entries = Optional.ofNullable(raft.getRaftLog().unstableEntries()).orElseGet(Vec::new)
                .stream()
                .map(Eraftpb.Entry.Builder::build)
                .collect(Collectors.toCollection(Vec::new));

        if (!raft.getMsgs().isEmpty()) {
            var raftMsg = raft.getMsgs();
            this.messages = raftMsg
                    .stream()
                    .map(Eraftpb.Message.Builder::build)
                    .collect(Collectors.toCollection(Vec::new));
            raftMsg.clear();
        } else {
            this.messages = new Vec<>();
        }

        this.committedEntries = Optional.ofNullable(sinceIdx)
                .map(idx -> raft.getRaftLog().nextEntriesSince(idx))
                .orElseGet(() -> Optional.ofNullable(raft.getRaftLog().nextEntries()).orElseGet(Vec::new))
                .stream()
                .map(Eraftpb.Entry.Builder::build)
                .collect(Collectors.toCollection(Vec::new));

        SoftState ss = raft.softState();
        if (ss != prevSs) {
            this.ss = ss;
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
        } else {
            this.snapshot = Eraftpb.Snapshot.getDefaultInstance();;
        }

        if (!raft.getReadStates().isEmpty()) {
            this.readStates = new Vec<>(raft.getReadStates());
        } else {
            this.readStates = new Vec<>();
        }
    }
}
