package com.stephen;

import eraftpb.Eraftpb;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Objects;

@Data
@AllArgsConstructor
public class RaftState {

    /// Contains the last meta information including commit index, the vote leader, and the vote term.
    private Eraftpb.HardState.Builder hardState;

    /// Records the current node IDs like `[1, 2, 3]` in the cluster. Every Raft node must have a
    /// unique ID in the cluster;
    private Eraftpb.ConfState.Builder confState;

    /// Indicates the `RaftState` is initialized or not.
    public boolean initialized() {
        return this.confState != null && Objects.equals(confState.build(), Eraftpb.ConfState.getDefaultInstance());
    }

}
