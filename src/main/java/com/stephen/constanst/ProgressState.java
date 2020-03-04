package com.stephen.constanst;

/// The state of the progress.
public enum ProgressState {
    /// Whether it's probing.(default)
    Probe,
    /// Whether it's replicating.
    Replicate,
    /// Whethers it's a snapshot.
    Snapshot,
}
