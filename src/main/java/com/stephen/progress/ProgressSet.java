package com.stephen.progress;

import com.stephen.QuorumFunction;
import com.stephen.QuorumUtils;
import com.stephen.constanst.CandidacyStatus;
import com.stephen.constanst.ProgressRole;
import com.stephen.exception.PanicException;
import com.stephen.exception.RaftError;
import com.stephen.exception.RaftErrorException;
import eraftpb.Eraftpb;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Getter
public class ProgressSet {

    private Map<Long, Progress> progress;

    /// The current configuration state of the cluster.
    private Configuration configuration;

    /// Create a progress set with the specified sizes already reserved.
    public ProgressSet(int voters, int learners) {
        this.progress = new HashMap<>(voters + learners);
        this.configuration = new Configuration(voters, learners);
    }

    private void clear() {
        this.progress.clear();
        this.configuration.getVoters().clear();
        this.configuration.getLearners().clear();
    }


    public void restoreSnapMeta(Eraftpb.SnapshotMetadata meta, long nextIdx, int maxInflight) {
        this.clear();

        for (Long id : meta.getConfState().getVotersList()) {
            this.progress.put(id, new Progress(nextIdx, maxInflight));
            this.configuration.getVoters().add(id);
        }

        for (Long id : meta.getConfState().getLearnersList()) {
            this.progress.put(id, new Progress(nextIdx, maxInflight));
            this.configuration.getLearners().add(id);
        }

        this.assertProgressAndConfigurationConsistent();
    }

    /// Returns the status of voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two quorums. Use `has_quorum` instead.
    public Map<Long, Progress> voters() {
        var set = this.voterIds();
        return this.progress.entrySet()
                .stream()
                .filter(s -> set.contains(s.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /// Returns the status of learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    public Map<Long, Progress> learners() {
        var set = this.learnerIds();
        return this.progress.entrySet()
                .stream()
                .filter(s -> set.contains(s.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    /// Returns the ids of all known voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two quorum. Use `has_quorum` instead.
    public Set<Long> voterIds() {
        return this.configuration.getVoters();
    }

    /// Returns the ids of all known learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two quorum. Use `has_quorum` instead.
    public Set<Long> learnerIds() {
        return this.configuration.getLearners();
    }

    /// Grabs a reference to the progress of a node. maybe null
    public Progress get(long id) {
        return this.progress.get(id);
    }


    /// Adds a voter or learner to the group.
    ///
    /// # Errors
    ///
    /// * `id` is in the voter set.
    /// * `id` is in the learner set.
    public void insertVoterOrLearner(long id, Progress pr, ProgressRole role) throws RaftErrorException {
        if (log.isDebugEnabled()) {
            log.debug("Inserting {} with id {}", role.name(), id);
        }

        if (this.learnerIds().contains(id)) {
            throw new RaftErrorException(RaftError.Exists, id, ProgressRole.VOTER.name());
        }

        if (this.voterIds().contains(id)) {
            throw new RaftErrorException(RaftError.Exists, id, ProgressRole.LEARNER.name());
        }

        var collection = switch (role) {
            case VOTER -> this.configuration.getVoters();
            case LEARNER -> this.configuration.getLearners();
        };

        collection.add(id);
        this.progress.put(id, pr);
        this.assertProgressAndConfigurationConsistent();
    }

    /// Removes the peer from the set of voters or learners.
    ///
    /// # Errors
    ///
    public Progress remove(long id) {
        if (log.isDebugEnabled()) {
            log.debug("Removing peer with id {}", id);
        }

        this.configuration.getVoters().remove(id);
        this.configuration.getLearners().remove(id);

        var removed = this.progress.get(id);

        this.assertProgressAndConfigurationConsistent();

        return removed;
    }

    /// Promote a learner to a peer.
    public void promoteLearner(long id) throws RaftErrorException {
        if (log.isDebugEnabled()) {
            log.debug("Promoting peer with id {}", id);
        }

        if (!this.configuration.getLearners().remove(id)) {
            // Wasn't already a learner. We can't promote what doesn't exist.
            throw new RaftErrorException(RaftError.NotExists, id, ProgressRole.LEARNER.name());
        }

        if (!this.configuration.getVoters().add(id)) {
            // Already existed, the caller should know this was a noop.
            throw new RaftErrorException(RaftError.NotExists, id, ProgressRole.VOTER.name());
        }

        this.assertProgressAndConfigurationConsistent();
    }


    private void assertProgressAndConfigurationConsistent() {
        if (this.voterIds().size() + this.learnerIds().size() != this.progress.size()) {
            throw new PanicException();
        }
    }

    /// Returns the maximal committed index for the cluster.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    public long maximalCommittedIndex(QuorumFunction qf) {

        var matched = this.configuration.getVoters()
                .stream()
                .map(this.progress::get)
                .map(Progress::getMatched)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());

        var quorum = QuorumUtils.calculateQuorum(qf, matched.size());

        return matched.get(quorum);

    }

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    public CandidacyStatus candidacyStatus(Map<Long, Boolean> votes, QuorumFunction quorumFunction) {
        var partitioningMap = votes.entrySet().stream()
                .collect(Collectors.partitioningBy(Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));

        if (this.configuration.hasQuorum(partitioningMap.get(true), quorumFunction)) {
            return CandidacyStatus.Elected;
        } else if (this.configuration.hasQuorum(partitioningMap.get(false), quorumFunction)) {
            return CandidacyStatus.Ineligible;
        } else {
            return CandidacyStatus.Eligible;
        }
    }

    /// Determines if the current quorum is active according to the this raft node.
    /// Doing this will set the `recent_active` of each peer to false.
    ///
    /// This should only be called by the leader.
    public boolean quorumRecentlyActive(long perspectiveOf, QuorumFunction qf) {
        var active = this.voters()
                .entrySet()
                .stream()
                .filter(s -> s.getKey() == perspectiveOf || s.getValue().isRecentActive())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        this.progress.forEach((k, v) -> v.setRecentActive(false));

        return this.configuration.hasQuorum(active, qf);
    }

    public boolean hasQuorum(Set<Long> potentialQuorum, QuorumFunction qf) {
        return this.configuration.hasQuorum(potentialQuorum, qf);
    }

    public void insertLearner(long id, Progress pr) throws RaftErrorException {
        log.debug("Inserting learner with id {}", id);

        if (this.learnerIds().contains(id)) {
            throw new RaftErrorException(RaftError.Exists, "learners");
        } else if (this.voterIds().contains(id)) {
            throw new RaftErrorException(RaftError.Exists, "voters");
        }

        this.configuration.getLearners().add(id);
        this.progress.put(id, pr);
        this.assertProgressAndConfigurationConsistent();
    }

    public void insertVoter(long id, Progress pr) throws RaftErrorException {
        log.debug("Inserting voter with id {}", id);

        if (this.learnerIds().contains(id)) {
            throw new RaftErrorException(RaftError.Exists, "learners");
        } else if (this.voterIds().contains(id)) {
            throw new RaftErrorException(RaftError.Exists, "voters");
        }

        this.configuration.getVoters().add(id);
        this.progress.put(id, pr);
        this.assertProgressAndConfigurationConsistent();
    }
}
