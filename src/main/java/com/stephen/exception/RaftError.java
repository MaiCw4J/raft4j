package com.stephen.exception;


import lombok.Getter;

public enum RaftError {

    /// The node exists, but should not.
    Exists("The node %d already exists in the %s set."),

    /// The node does not exist, but should.
    NotExists("The node %d is not in the %s set."),

    /// Raft cannot step the local message.
    StepLocalMsg("raft: cannot step raft local message"),

    /// The raft peer is not found and thus cannot step.
    StepPeerNotFound("raft: cannot step as peer not found"),

    /// The proposal of changes was dropped.
    ProposalDropped("raft: proposal dropped"),

    /// The configuration is invalid.
    ConfigInvalid("The configuration is invalid"),

    /// The storage was compacted and not accessible
    Storage_Compacted("log compacted"),

    /// The log is not available.
    Storage_Unavailable("log unavailable"),

    /// The snapshot is out of date.
    Storage_SnapshotOutOfDate("snapshot out of date"),

    /// The snapshot is being created.
    Storage_SnapshotTemporarilyUnavailable("snapshot is temporarily unavailable"),

    /// Some other error occurred.
    Storage_Other("unknown error");

    RaftError(String description) {
        this.description = description;
    }

    @Getter
    private String description;

}
