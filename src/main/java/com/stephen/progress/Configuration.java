package com.stephen.progress;

import com.stephen.QuorumFunction;
import com.stephen.QuorumUtils;
import com.stephen.constanst.ProgressRole;
import com.stephen.exception.*;
import eraftpb.Eraftpb;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

@Getter
public class Configuration {

    // The voter set.
    private Set<Long> voters;

    // The learner set.
    private Set<Long> learners;

    public Configuration(Set<Long> voters, Set<Long> learners) {
        this.voters = voters;
        this.learners = learners;
    }

    // Create a new `Configuration` from a given `ConfState`.
    public Configuration(Eraftpb.ConfState confState) {
        this.voters = new HashSet<>(confState.getVotersList());
        this.learners = new HashSet<>(confState.getLearnersList());
    }

    // Create a new `Configuration` with capacity
    public Configuration(int voters, int learners) {
        this.voters = new HashSet<>(voters);
        this.learners = new HashSet<>(learners);
    }

    /// Create a new `ConfState` from the configuration itself.
    public Eraftpb.ConfState toConfState() {
        return Eraftpb.ConfState.newBuilder()
                .addAllVoters(this.voters)
                .addAllLearners(this.learners)
                .build();
    }

    /// Validates that the configuration is not problematic.
    ///
    /// Namely:
    /// * There can be no overlap of voters and learners.
    /// * There must be at least one voter.
    public void valid() throws RaftErrorException {

        if (this.voters.isEmpty()) {
            throw new RaftErrorException(RaftError.ConfigInvalid, "There must be at least one voter.");
        }

        var retain = new HashSet<>(this.voters);
        retain.retainAll(this.learners);
        var iter = retain.iterator();
        if (iter.hasNext()) {
            throw new RaftErrorException(RaftError.Exists, iter.next(), ProgressRole.LEARNER.name());
        }

    }

    public boolean hasQuorum(Set<Long> potentialQuorum, QuorumFunction qf) {
        int votersSize = this.voters.size();
        var quorum = QuorumUtils.calculateQuorum(qf, votersSize);

        potentialQuorum.retainAll(this.voters);
        return potentialQuorum.size() >= quorum;
    }

    // Returns whether or not the given `id` is a member of this configuration.
    public boolean contains(long id) {
        return this.voters.contains(id) || this.learners.contains(id);
    }


}
