package com.stephen.progress;

import com.stephen.constanst.ProgressState;
import com.stephen.exception.PanicException;
import lombok.Getter;

import static com.stephen.constanst.Globals.INVALID_INDEX;

@Getter
public class Progress implements Cloneable {
    /// How much state is matched.
    private long matched;

    /// The next index to apply
    private long nextIdx;

    /// When in ProgressStateProbe, leader sends at most one replication message
    /// per heartbeat interval. It also probes actual progress of the follower.
    ///
    /// When in ProgressStateReplicate, leader optimistically increases next
    /// to the latest entry sent after sending replication message. This is
    /// an optimized state for fast replicating log entries to the follower.
    ///
    /// When in ProgressStateSnapshot, leader should have sent out snapshot
    /// before and stop sending any replication message.
    private ProgressState state;

    /// Paused is used in ProgressStateProbe.
    /// When Paused is true, raft should pause sending replication message to this peer.
    private boolean paused;

    /// This field is used in ProgressStateSnapshot.
    /// If there is a pending snapshot, the pendingSnapshot will be set to the
    /// index of the snapshot. If pendingSnapshot is set, the replication process of
    /// this Progress will be paused. raft will not resend snapshot until the pending one
    /// is reported to be failed.
    private long pendingSnapshot;

    /// This field is used in request snapshot.
    /// If there is a pending request snapshot, this will be set to the request
    /// index of the snapshot.
    private long pendingRequestSnapshot;


    /// This is true if the progress is recently active. Receiving any messages
    /// from the corresponding follower indicates the progress is active.
    /// RecentActive can be reset to false after an election timeout.
    private boolean recentActive;

    /// Inflights is a sliding window for the inflight messages.
    /// When inflights is full, no more message should be sent.
    /// When a leader sends out a message, the index of the last
    /// entry should be added to inflights. The index MUST be added
    /// into inflights in order.
    /// When a leader receives a reply, the previous inflights should
    /// be freed by calling inflights.freeTo.
    private Inflights ins;


    // Creates a new progress with the given settings.
    public Progress(long nextIdx, int insSize) {
        this.nextIdx = nextIdx;
        this.state = ProgressState.Probe;
        this.ins = new Inflights(insSize);
    }

    public void reset(long nextIdx) {
        this.matched = 0;
        this.nextIdx = nextIdx;
        this.state = ProgressState.Probe;
        this.paused = false;
        this.pendingSnapshot = 0;
        this.pendingRequestSnapshot = INVALID_INDEX;
        this.recentActive = false;
        this.ins.reset();
    }

    public void resetState(ProgressState state) {
        this.paused = false;
        this.pendingSnapshot = 0;
        this.state = state;
        this.ins.reset();
    }

    // Changes the progress to a probe.
    public void becomeProbe() {
        // If the original state is ProgressStateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if (this.state == ProgressState.Snapshot) {
            long pendingSnapshot = this.pendingSnapshot;
            this.resetState(ProgressState.Probe);
            this.nextIdx = Math.max(this.matched, pendingSnapshot) + 1;
        } else {
            this.resetState(ProgressState.Probe);
            this.nextIdx = this.matched + 1;
        }
    }

    // Changes the progress to a Replicate.
    public void becomeReplicate() {
        this.resetState(ProgressState.Replicate);
        this.nextIdx = this.matched + 1;
    }

    // Changes the progress to a snapshot.
    public void becomeSnapshot(long snapshotIdx) {
        this.resetState(ProgressState.Snapshot);
        this.pendingSnapshot = snapshotIdx;
    }

    // Sets the snapshot to failure.
    public void snapshotFailure() {
        this.pendingSnapshot = 0;
    }


    /// Unsets pendingSnapshot if Match is equal or higher than
    /// the pendingSnapshot
    public boolean maybeSnapshotAbort() {
        return this.state == ProgressState.Snapshot && this.matched >= this.pendingSnapshot;
    }

    /// Returns false if the given n index comes from an outdated message.
    /// Otherwise it updates the progress and returns true.
    public boolean maybeUpdate(long n) {
        var needUpdate = this.matched < n;

        if (needUpdate) {
            this.matched = n;
            this.resume();
        }

        if (this.nextIdx < n + 1) {
            this.nextIdx = n + 1;
        }

        return needUpdate;
    }


    /// Optimistically advance the index
    public void optimisticUpdate(long n) {
        this.nextIdx = n + 1;
    }

    /// Returns false if the given index comes from an out of order message.
    /// Otherwise it decreases the progress next index to min(rejected, last)
    /// and returns true.
    public boolean maybeDecrTo(long rejected, long last, long requestSnapshot) {
        if (this.state == ProgressState.Replicate) {
            // the rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            // Or rejected equals to matched and request_snapshot is the INVALID_INDEX.
            if (rejected < this.matched || (rejected == this.matched && requestSnapshot == INVALID_INDEX)) {
                return false;
            }

            if (requestSnapshot == INVALID_INDEX) {
                this.nextIdx = this.matched + 1;
            } else {
                this.pendingRequestSnapshot = requestSnapshot;
            }

            return true;
        }

        // The rejection must be stale if "rejected" does not match next - 1.
        // Do not consider it stale if it is a request snapshot message.
        if ((this.nextIdx == 0 || this.nextIdx - 1 != rejected) && requestSnapshot == INVALID_INDEX) {
            return false;
        }

        // Do not decrease next index if it's requesting snapshot.
        if (requestSnapshot == INVALID_INDEX) {
            var min = Math.min(rejected, last + 1);
            this.nextIdx = min < 1 ? 1 : min;
        } else if (this.pendingRequestSnapshot == INVALID_INDEX) {
            this.pendingRequestSnapshot = requestSnapshot;
        }

        this.resume();

        return true;
    }

    /// Determine whether progress is paused.
    public boolean isPaused() {
        return switch (this.state) {
            case Probe -> this.paused;
            case Replicate -> this.ins.full();
            case Snapshot -> true;
        };
    }

    // Resume progress
    public void resume() {
        this.paused = false;
    }

    /// Pause progress.
    public void pause() {
        this.paused = true;
    }

    /// Update inflight msgs and next_idx
    public void updateState(long last) {
        switch (this.state) {
            case Replicate -> {
                this.optimisticUpdate(last);
                this.ins.add(last);
            }
            case Probe -> this.pause();
            case Snapshot -> throw new PanicException(String.format("updating progress state in unhandled state %s", this.state.name()));
        }
    }


}
