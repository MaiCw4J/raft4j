package com.stephen.raft;

import com.stephen.Raft;
import com.stephen.constanst.StateRole;
import com.stephen.progress.ProgressSet;
import eraftpb.Eraftpb;
import lombok.Data;

@Data
public class StatusRef {

    /**
     * 当前节点的唯一id
     */
    private long id;

    /**
     * raft的状态，代表选举投票的状态
     */
    private Eraftpb.HardState hs;

    /**
     * The softstate of the raft, representing proposed state.
     */
    private SoftState ss;

    /**
     * The index of the last entry to have been applied.
     */
    private long applied;

    /**
     * The progress towards catching up and applying logs.
     */
    private ProgressSet progress;

    public StatusRef(Raft raft) {
        this.id = raft.getId();
        this.hs = raft.hardState();
        this.ss = raft.softState();
        this.applied = raft.getRaftLog().getApplied();
        if (this.getSs().getRaftState() == StateRole.Leader) {
            this.progress = raft.getPrs();
        }
    }

}
