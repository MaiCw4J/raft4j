package com.stephen;

import com.google.protobuf.ByteString;
import com.stephen.constanst.*;
import com.stephen.exception.PanicException;
import com.stephen.exception.RaftError;
import com.stephen.exception.RaftErrorException;
import com.stephen.lang.Vec;
import com.stephen.progress.Progress;
import com.stephen.progress.ProgressSet;
import eraftpb.Eraftpb;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.stephen.constanst.Globals.INVALID_ID;
import static com.stephen.constanst.Globals.INVALID_INDEX;
import static eraftpb.Eraftpb.MessageType.*;

@SuppressWarnings("StatementWithEmptyBody")
@Slf4j
@Data
public class Raft<T extends Storage> {

    // CAMPAIGN_PRE_ELECTION represents the first phase of a normal election when
    // Config.pre_vote is true.
    private static final ByteString CAMPAIGN_PRE_ELECTION = ByteString.copyFromUtf8("CampaignPreElection");

    // CAMPAIGN_ELECTION represents a normal (time-based) election (the second phase
    // of the election when Config.pre_vote is true).
    private static final ByteString CAMPAIGN_ELECTION = ByteString.copyFromUtf8("CampaignElection");

    // CAMPAIGN_TRANSFER represents the type of leader transfer.
    private static final ByteString CAMPAIGN_TRANSFER = ByteString.copyFromUtf8("CampaignTransfer");

    /// The current election term.
    private long term;

    /// Which peer this raft is voting for.
    private long vote;

    /// The ID of this node.
    private long id;

    /// The current read states.
    private Vec<ReadState> readStates;

    /// The persistent log.
    private RaftLog<T> raftLog;

    /// The maximum number of messages that can be inflight.
    private int maxInflight;

    /// The maximum length (in bytes) of all the entries.
    private long maxMsgSize;

    /// The peer is requesting snapshot, it is the index that the follower
    /// needs it to be included in a snapshot.
    private long pendingRequestSnapshot;

    private ProgressSet prs;

    /// The current role of this node.
    private StateRole state;


    /// Indicates whether state machine can be promoted to leader,
    /// which is true when it's a voter and its own id is in progress list.
    private boolean promotable;

    /// The current votes for this node in an election.
    ///
    /// Reset when changing role.
    private Map<Long, Boolean> votes;

    /// The list of messages.
    private Vec<Eraftpb.Message.Builder> msgs;

    /// The leader id
    private long leaderId;

    /// ID of the leader transfer target when its value is not None.
    ///
    /// If this is Some(id), we follow the procedure defined in raft thesis 3.10.
    private Long leadTransferee;

    /// Only one conf change may be pending (in the log, but not yet
    /// applied) at a time. This is enforced via `pending_conf_index`, which
    /// is set to a value >= the log index of the latest pending
    /// configuration change (if any). Config changes are only allowed to
    /// be proposed if the leader's applied index is greater than this
    /// value.
    ///
    /// This value is conservatively set in cases where there may be a configuration change pending,
    /// but scanning the log is possibly expensive. This implies that the index stated here may not
    /// necessarily be a config change entry, and it may not be a `BeginMembershipChange` entry, even if
    /// we set this to one.
    private long pendingConfIndex;

    /// The queue of read-only requests.
    private ReadOnly readOnly;

    /// Ticks since it reached last electionTimeout when it is leader or candidate.
    /// Number of ticks since it reached last electionTimeout or received a
    /// valid message from current leader when it is a follower.
    private int electionElapsed;

    /// Number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    private int heartbeatElapsed;

    /// Whether to check the quorum
    private boolean checkQuorum;

    /// Enable the pre vote algorithm.
    ///
    /// This enables a pre-election vote round on Candidates prior to disrupting the cluster.
    ///
    /// Enable this if greater cluster stability is preferred over faster elections.
    private boolean preVote;

    private boolean skipBCastCommit;
    private boolean batchAppend;

    private long heartbeatTimeout;
    private long electionTimeout;

    // randomized_election_timeout is a random number between
    // [min_election_timeout, max_election_timeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    private int randomizedElectionTimeout;
    private int minElectionTimeout;
    private int maxElectionTimeout;

    private QuorumFunction quorumFunction;

    public Raft(Config config, T store) throws RaftErrorException {
        config.validate();

        var raftState = store.initialState();

        var confState = raftState.getConfState();
        var voters = confState.getVotersList();
        var learners = confState.getLearnersList();

        this.id = config.getId();
        this.readStates = new Vec<>();
        this.raftLog = new RaftLog<>(store);
        this.maxInflight = config.getMaxInflightMsgs();
        this.maxMsgSize = config.getMaxSizePerMsg();
        this.prs = new ProgressSet(voters.size(), learners.size());
        this.pendingRequestSnapshot = INVALID_INDEX;
        this.state = StateRole.Follower;
        this.promotable = false;
        this.checkQuorum = config.isCheckQuorum();
        this.preVote = config.isPreVote();
        this.readOnly = new ReadOnly(config.getReadOnlyOption());
        this.heartbeatTimeout = config.getHeartbeatTick();
        this.electionTimeout = config.getElectionTick();
        this.votes = new HashMap<>();
        this.msgs = new Vec<>();
        this.leaderId = 0;
        this.leadTransferee = null;
        this.term = 0;
        this.electionElapsed = 0;
        this.pendingConfIndex = 0;
        this.vote = 0;
        this.heartbeatElapsed = 0;
        this.randomizedElectionTimeout = 0;
        this.minElectionTimeout = config.minElectionTick();
        this.maxElectionTimeout = config.maxElectionTick();
        this.skipBCastCommit = config.isSkipBcastCommit();
        this.batchAppend = config.isBatchAppend();
        this.quorumFunction = config.getQuorumFunction();

        for (Long voterId : voters) {
            var pr = new Progress(1, this.maxInflight);
            try {
                this.prs.insertVoter(voterId, pr);
            } catch (RaftErrorException e) {
                throw new PanicException(log, e);
            }
            this.promotable = voterId == this.id;
        }

        for (Long learnerId : learners) {
            var pr = new Progress(1, this.maxInflight);
            try {
                this.prs.insertLearner(learnerId, pr);
            } catch (RaftErrorException e) {
                throw new PanicException(log, e);
            }
        }
        var rs = raftState.getHardState();
        if (rs != null && Objects.equals(rs.build(), Eraftpb.HardState.getDefaultInstance())) {
            this.loadState(rs);
        }

        if (config.getApplied() > 0) {
            this.commitApply(config.getApplied());
        }

        this.becomeFollower(this.term, INVALID_ID);

        log.info("new raft: term {}, commit {}, applied {}, last index {}, last term {}, peers {}",
                this.term,
                this.raftLog.getCommitted(),
                this.raftLog.getApplied(),
                this.raftLog.lastIndex(),
                this.raftLog.lastTerm(),
                this.prs.voterIds());

    }

    private Eraftpb.Message.Builder newMessage(long to, Eraftpb.MessageType fieldType, Long from) {
        var builder = Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(fieldType);
        if (from != null) {
            builder.setFrom(from);
        }
        return builder;
    }

    /// Maps vote and pre_vote message types to their correspond responses.
    public Eraftpb.MessageType voteRespMsgType(Eraftpb.MessageType t) {
        return switch (t) {
            case MsgRequestVote -> Eraftpb.MessageType.MsgRequestVoteResponse;
            case MsgRequestPreVote -> Eraftpb.MessageType.MsgRequestPreVoteResponse;
            default -> throw new PanicException("Not a vote message: " + t.name());
        };
    }


    // send persists state to stable storage and then sends to its mailbox.
    public void send(Eraftpb.Message.Builder toSend) {
        if (log.isDebugEnabled()) {
            log.debug("Sending from {} to {} msgType {}", this.id, toSend.getTo(), toSend.getMsgType());
        }
        var type = toSend.getMsgType();
        switch (type) {
            case MsgRequestVote, MsgRequestPreVote, MsgRequestVoteResponse, MsgRequestPreVoteResponse -> {
                if (toSend.getTerm() == 0) {
                    // All {pre-,}campaign messages need to have the term set when
                    // sending.
                    // - MsgVote: m.Term is the term the node is campaigning for,
                    //   non-zero as we increment the term when campaigning.
                    // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                    //   granted, non-zero for the same reason MsgVote is
                    // - MsgPreVote: m.Term is the term the node will campaign,
                    //   non-zero as we use m.Term to indicate the next term we'll be
                    //   campaigning for
                    // - MsgPreVoteResp: m.Term is the term received in the original
                    //   MsgPreVote if the pre-vote was granted, non-zero for the
                    //   same reasons MsgPreVote is
                    throw new PanicException(log, "term should be set when sending {}", type.name());
                }
            }
            default -> {
                if (toSend.getTerm() != 0) {
                    throw new PanicException(log, "term should not be set when sending {:?} (was {})", type.name(), toSend.getTerm());
                }
                // do not attach term to MsgPropose, MsgReadIndex
                // proposals are a way to forward to the leader and
                // should be treated as local message.
                // MsgReadIndex is also forwarded to leader.
                if (type != MsgPropose && type != MsgReadIndex) {
                    toSend.setTerm(this.term);
                }
            }
        }
        this.msgs.add(toSend.setFrom(this.id));
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean prepareSendSnapshot(Eraftpb.Message.Builder builder, Progress pr, long to) {
        if (pr.isRecentActive()) {
            log.info("ignore sending snapshot to {} since it is not recently active", to);
            return false;
        }

        builder.setMsgType(MsgSnapshot);

        try {
            var snapshot = this.raftLog.snapshot(pr.getPendingRequestSnapshot());

            if (snapshot.getMetadata().getIndex() == 0) {
                throw new PanicException(log, "need non-empty snapshot");
            }

            builder.setSnapshot(snapshot);
            var snapshotIdx = snapshot.getMetadata().getIndex();
            if (log.isDebugEnabled()) {
                log.debug("[firstIndex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {}",
                        this.raftLog.firstIndex(),
                        this.raftLog.getCommitted(),
                        snapshotIdx,
                        snapshot.getMetadata().getTerm(),
                        to);
            }

            pr.becomeSnapshot(snapshotIdx);

            if (log.isDebugEnabled()) {
                log.debug("paused sending replication messages to {}", to);
            }

            return true;
        } catch (RaftErrorException e) {
            if (e.getError() == RaftError.Storage_SnapshotTemporarilyUnavailable) {
                log.info("failed to send snapshot to {} because snapshot is temporarily unavailable", to);
                return false;
            } else {
                throw new PanicException(log, "unexpected error", e);
            }
        }
    }


    /// Sends RPC, with entries to the given peer.
    public void sendAppend(long to, Progress pr) {
        if (pr.isPaused()) {
            return;
        }

        var builder = Eraftpb.Message.newBuilder().setTo(to);

        if (pr.getPendingRequestSnapshot() != Globals.INVALID_INDEX) {
            // Check pending request snapshot first to avoid unnecessary loading entries.
            if (!this.prepareSendSnapshot(builder, pr, to)) {
                return;
            }
        } else {
            try {
                var term = this.raftLog.term(pr.getNextIdx() - 1);
                var entries = this.raftLog.entries(pr.getNextIdx(), this.maxMsgSize);

                if (this.batchAppend && this.tryBatching(to, pr, entries)) {
                    return;
                }

                this.prepareSendEntries(builder, pr, term, entries);
            } catch (RaftErrorException e) {
                // send snapshot if we failed to get term or entries.
                if (!this.prepareSendSnapshot(builder, pr, to)) {
                    return;
                }
            }
        }

        this.send(builder);
    }

    // send_heartbeat sends an empty MsgAppend
    private void sendHeartbeat(long to, Progress pr, ByteString ctx) {
        // Attach the commit as min(to.matched, self.raft_log.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        var builder = Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(MsgHeartbeat)
                .setCommit(Math.min(pr.getMatched(), this.raftLog.getCommitted()));
        if (ctx != null) {
            builder.setContext(ctx);
        }
        this.send(builder);
    }

    private void prepareSendEntries(Eraftpb.Message.Builder builder, Progress pr, long term, List<Eraftpb.Entry.Builder> entries) {
        builder.setMsgType(MsgAppend)
                .setIndex(pr.getNextIdx() - 1)
                .setLogTerm(term)
                .setCommit(this.raftLog.getCommitted())
                .clearEntries();
        if ($.isNotEmpty(entries)) {
            for (Eraftpb.Entry.Builder entry : entries) {
                builder.addEntries(entry);
            }

            var last = entries.get(entries.size() - 1).getIndex();
            pr.updateState(last);
        }
    }


    private boolean tryBatching(long to, Progress pr, List<Eraftpb.Entry.Builder> entries) {
        // if MsgAppend for the receiver already exists, try_batching
        // will append the entries to the existing MsgAppend
        for (Eraftpb.Message.Builder msg : this.msgs) {
            if (msg.getMsgType() == MsgAppend && msg.getTo() == to) {
                if (entries != null && !entries.isEmpty()) {
                    if (!$.isContinuousEntries(msg.getEntriesList(), entries)) {
                        return false;
                    }
                    var batchedEntries = msg.getEntriesBuilderList();
                    batchedEntries.addAll(entries);

                    var lastIdx = batchedEntries.get(batchedEntries.size() - 1).getIndex();
                    pr.updateState(lastIdx);
                }
                break;
            }
        }
        return true;
    }

    /// Returns true to indicate that there will probably be some readiness need to be handled.
    public boolean tick() {
        return switch (this.state) {
            case Leader -> this.tickHeartbeat();
            case Follower, PreCandidate, Candidate -> this.tickElection();
        };
    }

    /// Run by followers and candidates after self.election_timeout.
    ///
    /// Returns true to indicate that there will probably be some readiness need to be handled.
    private boolean tickElection() {
        this.electionElapsed++;
        if (!this.passElectionTimeout() || !this.promotable) {
            return false;
        }

        this.electionElapsed = 0;
        var toSend = newMessage(INVALID_ID, MsgHup, this.id);
        try {
            this.step(toSend);
        } catch (RaftErrorException ignored) {
        }
        return true;
    }

    // tick_heartbeat is run by leaders to send a MsgBeat after self.heartbeat_timeout.
    // Returns true to indicate that there will probably be some readiness need to be handled.
    private boolean tickHeartbeat() {
        this.heartbeatElapsed++;
        this.electionElapsed++;

        boolean hasReady = false;
        if (this.electionElapsed >= this.electionTimeout) {
            this.electionElapsed = 0;
            if (this.checkQuorum) {
                var quorumMsg = newMessage(INVALID_ID, MsgCheckQuorum, this.id);
                hasReady = true;
                try {
                    this.step(quorumMsg);
                } catch (RaftErrorException ignored) {
                }
            }
            if (this.state == StateRole.Leader && this.leadTransferee != null) {
                this.abortLeaderTransfer();
            }
        }

        if (this.state != StateRole.Leader) {
            return hasReady;
        }

        if (this.heartbeatElapsed > this.heartbeatTimeout) {
            this.heartbeatElapsed = 0;
            hasReady = true;
            var beatMsg = newMessage(INVALID_ID, MsgBeat, this.id);
            try {
                this.step(beatMsg);
            } catch (RaftErrorException ignored) {
            }
        }
        return hasReady;
    }

    /// `pass_election_timeout` returns true iff `election_elapsed` is greater
    /// than or equal to the randomized election timeout in
    /// [`election_timeout`, 2 * `election_timeout` - 1].
    public boolean passElectionTimeout() {
        return this.electionElapsed >= this.randomizedElectionTimeout;
    }

    public void abortLeaderTransfer() {
        this.leadTransferee = null;
    }

    /// Steps the raft along via a message. This should be called every time your raft receives a
    /// message from a peer.
    public void step(Eraftpb.Message.Builder m) throws RaftErrorException {
        var msgType = m.getMsgType();
        // Handle the message term, which may result in our stepping down to a follower.
        if (m.getTerm() == 0) {
            // local message
        } else if (m.getTerm() > this.term) {

            if (msgType == MsgRequestVote || msgType == MsgRequestPreVote) {

                boolean force = CAMPAIGN_TRANSFER.equals(m.getContext());
                boolean inLease = this.checkQuorum &&
                        this.leaderId != INVALID_ID &&
                        this.electionElapsed < this.electionTimeout;
                if (!force && inLease) {
                    // if a server receives RequestVote request within the minimum election
                    // timeout of hearing from a current leader, it does not update its term
                    // or grant its vote
                    //
                    // This is included in the 3rd concern for Joint Consensus, where if another
                    // peer is removed from the cluster it may try to hold elections and disrupt
                    // stability.
                    log.info("[log term: {}, index: {}, vote: {}] ignored vote from {} " +
                                    "[log term: {}, index: {}]: lease is not expired; " +
                                    "term {}, remaining ticks {}, msg type {}",
                            this.raftLog.lastTerm(),
                            this.raftLog.lastIndex(),
                            this.vote,
                            m.getFrom(),
                            m.getLogTerm(),
                            m.getIndex(),
                            this.term,
                            this.electionTimeout - this.electionElapsed,
                            msgType);
                    return;
                }
            }

            if (msgType == MsgRequestPreVote || (msgType == MsgRequestPreVoteResponse && !m.getReject())) {
                // For a pre-vote request:
                // Never change our term in response to a pre-vote request.
                //
                // For a pre-vote response with pre-vote granted:
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
            } else {
                log.info("received a message with higher term from {}, term {}, message_term {}, msg type {}",
                        m.getFrom(),
                        this.term,
                        m.getTerm(),
                        m.getMsgType());

                var from = switch (msgType) {
                    case MsgAppend, MsgHeartbeat, MsgSnapshot -> m.getFrom();
                    default -> INVALID_ID;
                };

                this.becomeFollower(m.getTerm(), from);
            }
        } else if (m.getTerm() < this.term) {
            if (this.checkQuorum || this.preVote && (msgType == MsgHeartbeat || msgType == MsgAppend)) {
                // We have received messages from a leader at a lower term. It is possible
                // that these messages were simply delayed in the network, but this could
                // also mean that this node has advanced its term number during a network
                // partition, and it is now unable to either win an election or to rejoin
                // the majority on the old term. If checkQuorum is false, this will be
                // handled by incrementing term numbers in response to MsgVote with a higher
                // term, but if checkQuorum is true we may not advance the term on MsgVote and
                // must generate other messages to advance the term. The net result of these
                // two features is to minimize the disruption caused by nodes that have been
                // removed from the cluster's configuration: a removed node will send MsgVotes
                // which will be ignored, but it will not receive MsgApp or MsgHeartbeat, so it
                // will not create disruptive term increases, by notifying leader of this node's
                // activeness.
                // The above comments also true for Pre-Vote
                //
                // When follower gets isolated, it soon starts an election ending
                // up with a higher term than leader, although it won't receive enough
                // votes to win the election. When it regains connectivity, this response
                // with "pb.MsgAppResp" of higher term would force leader to step down.
                // However, this disruption is inevitable to free this stuck node with
                // fresh election. This can be prevented with Pre-Vote phase.
                var resp = newMessage(m.getFrom(), MsgAppendResponse, null);
                this.send(resp);
            } else if (msgType == MsgRequestPreVote) {
                // Before pre_vote enable, there may be a recieving candidate with higher term,
                // but less log. After update to pre_vote, the cluster may deadlock if
                // we drop messages with a lower term.
                log.info("{} [log_term: {}, index: {}, vote: {}] rejected {} from {} [log_term: {}, index: {}] at term {}",
                        this.id,
                        this.raftLog.lastTerm(),
                        this.raftLog.lastIndex(),
                        this.vote,
                        msgType,
                        m.getFrom(),
                        m.getLogTerm(),
                        m.getIndex(),
                        this.term);

                var builder = newMessage(m.getFrom(), MsgRequestPreVoteResponse, null)
                        .setTerm(this.term).setReject(true);
                this.send(builder);
            } else {
                // ignore other cases
                log.info("ignored a message with lower term from {}, term {}, msg type {}, msg term {}",
                        m.getFrom(),
                        this.term,
                        msgType,
                        m.getTerm());
            }
            return;
        }

        switch (msgType) {
            case MsgHup -> this.hup(false);
            case MsgRequestVote, MsgRequestPreVote -> {
                // We can vote if this is a repeat of a vote we've already cast...
                boolean canVote = (this.vote == m.getFrom()) ||
                        // ...we haven't voted and we don't think there's a leader yet in this term...
                        (this.vote == INVALID_ID && this.leaderId == INVALID_ID) ||
                        // ...or this is a PreVote for a future term...
                        (msgType == MsgRequestPreVote && m.getTerm() > this.term);

                if (canVote && this.raftLog.isUpToDate(m.getIndex(), m.getLogTerm())) {
                    // When responding to Msg{Pre,}Vote messages we include the term
                    // from the message, not the local term. To see why consider the
                    // case where a single node was previously partitioned away and
                    // it's local term is now of date. If we include the local term
                    // (recall that for pre-votes we don't update the local term), the
                    // (pre-)campaigning node on the other end will proceed to ignore
                    // the message (it ignores all out of date messages).
                    // The term in the original message and current local term are the
                    // same in the case of regular votes, but different for pre-votes.
                    this.logVoteApprove(m);

                    var resp = newMessage(m.getFrom(), voteRespMsgType(msgType), null)
                            .setReject(false)
                            .setTerm(m.getTerm());

                    if (msgType == MsgRequestVote) {
                        // Only record real votes.
                        this.electionElapsed = 0;
                        this.vote = m.getFrom();
                    }

                    this.send(resp);
                } else {
                    this.logVoteReject(m);
                    var resp = newMessage(m.getFrom(), voteRespMsgType(msgType), null)
                            .setReject(true)
                            .setTerm(this.term);

                    this.send(resp);
                }
            }
            default -> {
                switch (this.state) {
                    case Candidate, PreCandidate -> this.stepCandidate(m);
                    case Follower -> this.stepFollower(m);
                    case Leader -> this.stepLeader(m);
                }
            }
        }
    }

    private void stepLeader(Eraftpb.Message.Builder m) throws RaftErrorException {
        switch (m.getMsgType()) {
            case MsgBeat -> {
                this.bcastHeartbeat();
                return;
            }
            case MsgCheckQuorum -> {
                if (!this.checkQuorumActive()) {
                    log.warn("stepped down to follower since quorum is not active");
                    this.becomeFollower(this.term, INVALID_ID);
                }
                return;
            }
            case MsgPropose -> {
                if (m.getEntriesCount() == 0) {
                    throw new PanicException(log, "stepped empty MsgProp");
                }
                if (!this.prs.voterIds().contains(this.id)) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    throw new RaftErrorException(RaftError.ProposalDropped);
                }

                if (this.leadTransferee != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("[term {}] transfer leadership to {} is in progress; dropping proposal",
                                this.term,
                                this.leadTransferee);
                    }
                    throw new RaftErrorException(RaftError.ProposalDropped);
                }

                for (int i = 0; i < m.getEntriesCount(); i++) {
                    var e = m.getEntries(i);
                    if (e.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                        if (this.hasPendingConf()) {
                            log.info("propose conf entry ignored since pending unApplied configuration index {}, applied {}",
                                    this.pendingConfIndex,
                                    this.raftLog.getApplied());
                            m.setEntries(i, Eraftpb.Entry.newBuilder().setEntryType(Eraftpb.EntryType.EntryNormal));
                        } else {
                            this.pendingConfIndex = this.raftLog.lastIndex() + i + 1;
                        }
                    }
                }
                this.appendEntry(m.getEntriesBuilderList());
                this.bcastAppend();
                return;
            }
            case MsgReadIndex -> {
                long term;
                try {
                    term = this.raftLog.term(this.raftLog.getCommitted());
                } catch (RaftErrorException e) {
                    term = 0;
                }
                if (term != this.term) {
                    // Reject read only request when this leader has not committed any log entry
                    // in its term.
                    return;
                }

                if (!this.prs.hasQuorum(Set.of(this.id), this.quorumFunction)) {
                    // thinking: use an interally defined context instead of the user given context.
                    // We can express this in terms of the term and index instead of
                    // a user-supplied value.
                    // This would allow multiple reads to piggyback on the same message.
                    switch (this.readOnly.getOption()) {
                        case Safe -> {
                            this.readOnly.addRequest(this.raftLog.getCommitted(), m.build());
                            this.bcastHeartbeatWithCtx(m.getEntries(0).getData());
                        }
                        case LeaseBased -> {
                            var readIndex = this.raftLog.getCommitted();
                            if (m.getFrom() == INVALID_ID || m.getFrom() == this.id) {
                                // from local member
                                var rs = new ReadState(readIndex, m.getEntries(0).getData());
                                this.readStates.add(rs);
                            } else {
                                var toSend = Eraftpb.Message.newBuilder()
                                        .setMsgType(MsgReadIndexResp)
                                        .setTo(m.getFrom())
                                        .setIndex(readIndex);
                                for (Eraftpb.Entry entry : m.getEntriesList()) {
                                    toSend.addEntries(entry);
                                }
                                this.send(toSend);
                            }

                        }
                    }
                } else {
                    // there is only one voting member (the leader) in the cluster
                    if (m.getFrom() == INVALID_ID || m.getFrom() == this.id) {
                        // from leader itself
                        var rs = new ReadState(this.raftLog.getCommitted(), m.getEntries(0).getData());
                        this.readStates.add(rs);
                    } else {
                        // from learner member
                        var toSend = Eraftpb.Message.newBuilder()
                                .setMsgType(MsgReadIndexResp)
                                .setTo(m.getFrom())
                                .setIndex(this.raftLog.getCommitted())
                                .addAllEntries(m.getEntriesList());
                        this.send(toSend);
                    }
                }
                return;
            }
        }

        var from = this.prs.get(m.getFrom());
        if (from == null) {
            if (log.isDebugEnabled()) {
                log.debug("no progress available for {}", m.getFrom());
            }
            return;
        }

        boolean sendAppend = false;
        boolean maybeCommit = false;
        boolean oldPaused = false;
        List<Eraftpb.Message.Builder> moreToSend = new ArrayList<>();

        // response handle
        out: switch (m.getMsgType()) {
            case MsgAppendResponse -> {
                from.setRecentActive(true);

                if (m.getReject()) {
                    if (log.isDebugEnabled()) {
                        log.debug("received msgAppend rejection last index {}, from {}, index {}",
                                m.getRejectHint(),
                                m.getFrom(),
                                m.getIndex());
                    }
                    if (from.maybeDecrTo(m.getIndex(), m.getRejectHint(), m.getRequestSnapshot())) {
                        if (log.isDebugEnabled()) {
                            log.debug("decreased progress of {}", from);
                        }
                        if (from.getState() == ProgressState.Replicate) {
                            from.becomeProbe();
                        }
                        sendAppend = true;
                        break;
                    }
                }
                oldPaused = from.isPaused();
                if (!from.maybeUpdate(m.getIndex())) {
                    break;
                }

                // Transfer leadership is in progress.
                boolean isTimeoutNow = this.leadTransferee != null &&
                        this.leadTransferee == m.getFrom() &&
                        from.getMatched() == this.raftLog.lastIndex();
                if (isTimeoutNow) {
                    log.info("sent MsgTimeoutNow to {} after received MsgAppResp", m.getFrom());
                    this.sendTimeoutNow(m.getFrom());
                }

                switch (from.getState()) {
                    case Probe -> from.becomeReplicate();
                    case Snapshot -> {
                        if (!from.maybeSnapshotAbort()) {
                            break out;
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("snapshot aborted, resumed sending replication messages to {}", m.getFrom());
                        }
                        from.becomeProbe();
                    }
                    case Replicate -> from.getIns().freeTo(m.getIndex());
                }
                maybeCommit = true;
            }
            case MsgHeartbeatResponse -> {
                // Update the node. Drop the value explicitly since we'll check the qourum after.
                from.setRecentActive(true);
                from.resume();

                // free one slot for the full inflights window to allow progress.
                if (from.getState() == ProgressState.Replicate && from.getIns().full()) {
                    from.getIns().freeFirstOne();
                }

                // Does it request snapshot?
                if (from.getMatched() < this.raftLog.lastIndex() || from.getPendingRequestSnapshot() != INVALID_ID) {
                    sendAppend = true;
                }

                if (this.readOnly.getOption() != ReadOnlyOption.Safe || m.getContext().isEmpty()) {
                    break;
                }

                if (this.prs.hasQuorum(this.readOnly.recvAck(m), this.quorumFunction)) {
                    break;
                }

                var rss = this.readOnly.advance(m.getContext());

                for (ReadIndexStatus rs : rss) {
                    var req = rs.getReq();
                    if (req.getFrom() == INVALID_ID || req.getFrom() == this.id) {
                        // from local member
                        var newRs = new ReadState(rs.getIndex(), req.getEntries(0).getData());
                        this.readStates.add(newRs);
                    } else {
                        var toSend = Eraftpb.Message.newBuilder()
                                .setMsgType(MsgReadIndexResp)
                                .setTo(req.getFrom())
                                .setIndex(rs.getIndex())
                                .addAllEntries(req.getEntriesList());
                        moreToSend.add(toSend);
                    }
                }
            }
            case MsgSnapStatus -> {
                if (from.getState() == ProgressState.Snapshot) {
                    if (m.getReject()) {
                        if (log.isDebugEnabled()) {
                            log.debug("snapshot failed, resumed sending replication messages to {}", m.getFrom());
                        }
                        from.snapshotFailure();
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("snapshot succeeded, resumed sending replication messages to {}", m.getFrom());
                        }
                    }
                    from.becomeProbe();
                    // If snapshot finish, wait for the msgAppResp from the remote node before sending
                    // out the next msgAppend.
                    // If snapshot failure, wait for a heartbeat interval before next try
                    from.pause();
                    from.setPendingRequestSnapshot(INVALID_INDEX);
                }
            }
            case MsgUnreachable -> {
                // During optimistic replication, if the remote becomes unreachable,
                // there is huge probability that a MsgAppend is lost.
                if (from.getState() != ProgressState.Replicate) {
                    if (log.isDebugEnabled()) {
                        log.debug("failed to send message to {} because it is unreachable", m.getFrom());
                    }
                    break;
                }
                from.becomeProbe();
            }
            case MsgTransferLeader -> {
                var leadTransferee = m.getFrom();
                if (this.prs.learnerIds().contains(leadTransferee)) {
                    if (log.isDebugEnabled()) {
                        log.debug("ignored transferring leadership");
                    }
                    break;
                }
                var lastLeadTransferee = this.leadTransferee;
                if (lastLeadTransferee != null) {
                    if (lastLeadTransferee == leadTransferee) {
                        log.info("[term {}] transfer leadership to {} is in progress, ignores request to same node",
                                this.term,
                                leadTransferee);
                        break;
                    }
                    this.abortLeaderTransfer();
                    log.info("[term {}] abort previous transferring leadership to {}", this.term, lastLeadTransferee);
                }
                if (leadTransferee == this.id) {
                    if (log.isDebugEnabled()) {
                        log.debug("already leader; ignored transferring leadership to self");
                    }
                    break;
                }
                // Transfer leadership to third party.
                log.info("[term {}] starts to transfer leadership to {}", this.term, leadTransferee);
                // Transfer leadership should be finished in one electionTimeout
                // so reset r.electionElapsed.
                this.electionElapsed = 0;
                this.leadTransferee = leadTransferee;
                var pr = this.prs.get(leadTransferee);
                if (pr.getMatched() == this.raftLog.lastIndex()) {
                    log.info("sends MsgTimeoutNow to {} immediately as {} already has up-to-date log",
                            leadTransferee,
                            leadTransferee);
                    this.sendTimeoutNow(leadTransferee);
                } else {
                    this.sendAppend(leadTransferee, pr);
                }
            }
        }

        if (maybeCommit) {
            if (this.maybeCommit()) {
                if (this.shouldBcastCommit()) {
                    this.bcastAppend();
                }
            } else if (oldPaused) {
                // update() reset the wait state on this node. If we had delayed sending
                // an update before, send it now.
                sendAppend = true;
            }
        }

        if (sendAppend) {
            this.sendAppend(m.getFrom(), this.prs.get(m.getFrom()));
        }

        if (!moreToSend.isEmpty()) {
            moreToSend.forEach(this::send);
        }
    }

    // step_candidate is shared by state Candidate and PreCandidate; the difference is
    // whether they respond to MsgRequestVote or MsgRequestPreVote.
    private void stepCandidate(Eraftpb.Message.Builder m) throws RaftErrorException {
        switch (m.getMsgType()) {
            case MsgPropose -> {
                log.info("no leader at term {}; dropping proposal", this.term);
                throw new RaftErrorException(RaftError.ProposalDropped);
            }
            case MsgAppend -> {
                this.becomeFollower(m.getTerm(), m.getFrom());
                this.handleAppendEntries(m);
            }
            case MsgHeartbeat -> {
                this.becomeFollower(m.getTerm(), m.getFrom());
                this.handleHeartbeat(m);
            }
            case MsgSnapshot -> {
                this.becomeFollower(m.getTerm(), m.getFrom());
                this.handleSnapshot(m.build());
            }
            case MsgRequestVoteResponse, MsgRequestPreVoteResponse -> {
                // Only handle vote responses corresponding to our candidacy (while in
                // state Candidate, we may get stale MsgPreVoteResp messages in this term from
                // our pre-candidate state).
                if ((this.state == StateRole.PreCandidate && m.getMsgType() != MsgRequestPreVoteResponse) ||
                        (this.state == StateRole.Candidate && m.getMsgType() != MsgRequestVoteResponse)) {
                    return;
                }

                this.registerVote(m.getFrom(), !m.getReject());

                switch (this.prs.candidacyStatus(this.votes, this.quorumFunction)) {
                    case Elected -> {
                        if (this.state == StateRole.PreCandidate) {
                            this.campaign(CAMPAIGN_ELECTION);
                        } else {
                            this.becomeLeader();
                            this.bcastAppend();
                        }
                    }
                    case Ineligible -> this.becomeFollower(this.term, INVALID_ID);
                    case Eligible -> {
                    }
                }
            }
            case MsgTimeoutNow -> {
                if (log.isDebugEnabled()) {
                    log.debug("{} ignored MsgTimeoutNow from {}", this.term, m.getFrom());
                }
            }
        }
    }

    private void stepFollower(Eraftpb.Message.Builder m) throws RaftErrorException {
        switch (m.getMsgType()) {
            case MsgPropose -> {
                if (this.leaderId == INVALID_ID) {
                    log.info("no leader at term {}; dropping proposal", this.term);
                    throw new RaftErrorException(RaftError.ProposalDropped);
                }
                m.setTo(this.leaderId);
                this.send(m);
            }
            case MsgAppend -> {
                this.electionElapsed = 0;
                this.leaderId = m.getFrom();
                this.handleAppendEntries(m);
            }
            case MsgHeartbeat -> {
                this.electionElapsed = 0;
                this.leaderId = m.getFrom();
                this.handleHeartbeat(m);
            }
            case MsgSnapshot -> {
                this.electionElapsed = 0;
                this.leaderId = m.getFrom();
                this.handleSnapshot(m.build());
            }
            case MsgTransferLeader -> {
                if (this.leaderId == INVALID_ID) {
                    log.info("no leader at term {}; dropping leader transfer msg", this.term);
                    return;
                }
                m.setTo(this.leaderId);
                this.send(m);
            }
            case MsgTimeoutNow -> {
                if (this.promotable) {
                    log.info("[term {}] received MsgTimeoutNow from {} and starts an election to get leadership.",
                            this.term,
                            m.getFrom());
                    // Leadership transfers never use pre-vote even if self.pre_vote is true; we
                    // know we are not recovering from a partition so there is no need for the
                    // extra round trip.
                    this.hup(true);
                } else {
                    log.info("received MsgTimeoutNow from {} but is not promotable", m.getFrom());
                }
            }
            case MsgReadIndex -> {
                if (this.leaderId == INVALID_ID) {
                    log.info("no leader at term {}; dropping index reading msg", this.term);
                    return;
                }
                m.setTo(this.leaderId);
                this.send(m);
            }
            case MsgReadIndexResp -> {
                var size = m.getEntriesList().size();
                if (size != 1) {
                    log.error("invalid format of MsgReadIndexResp from {} entries count {}", m.getFrom(), size);
                    return;
                }

                var rs = new ReadState(m.getIndex(), m.getEntries(0).getData());
                this.readStates.add(rs);
            }
        }
    }

    /// Converts this node to a follower.
    public void becomeFollower(long term, long leaderId) {
        long pendingRequestSnapshot = this.pendingRequestSnapshot;
        this.reset(term);
        this.leaderId = leaderId;
        this.state = StateRole.Follower;
        this.pendingRequestSnapshot = pendingRequestSnapshot;
        log.info("became follower at term {}", this.term);
    }

    // TODO: revoke pub when there is a better way to test.
    /// Converts this node to a candidate
    ///
    /// # Panics
    ///
    /// Panics if a leader already exists.
    private void becomeCandidate() {
        if (this.state == StateRole.Leader) {
            throw new PanicException("invalid transition [leader -> candidate]");
        }

        var term = this.term + 1;
        this.reset(term);
        this.vote = this.id;
        this.state = StateRole.Candidate;
        log.info("became candidate at term {}", term);
    }

    /// Converts this node to a pre-candidate
    ///
    /// # Panics
    ///
    /// Panics if a leader already exists.
    private void becomePreCandidate() {
        if (this.state == StateRole.Leader) {
            throw new PanicException("invalid transition [leader -> pre-candidate]");
        }

        // Becoming a pre-candidate changes our state.
        // but doesn't change anything else. In particular it does not increase
        // self.term or change self.vote.
        this.state = StateRole.PreCandidate;
        this.votes.clear();
        // If a network partition happens, and leader is in minority partition,
        // it will step down, and become follower without notifying others.
        this.leaderId = INVALID_ID;
        log.info("became pre-candidate at term {}", this.term);
    }

    // TODO: revoke pub when there is a better way to test.
    /// Makes this raft the leader.
    ///
    /// # Panics
    ///
    /// Panics if this is a follower node.
    private void becomeLeader() {
        if (log.isTraceEnabled()) {
            log.trace("ENTER become_leader");
        }

        if (this.state == StateRole.Follower) {
            throw new PanicException("invalid transition [follower -> leader]");
        }
        var term = this.term;
        this.reset(term);
        this.leaderId = this.id;
        this.state = StateRole.Leader;

        // Followers enter replicate mode when they've been successfully probed
        // (perhaps after having received a snapshot as a result). The leader is
        // trivially in this state. Note that r.reset() has initialized this
        // progress with the last index already.
        var selfId = this.id;
        this.prs.get(selfId).becomeReplicate();
        this.pendingConfIndex = this.raftLog.lastIndex();

        this.appendEntry(List.of(Eraftpb.Entry.newBuilder()));

        log.info("became leader at term {}", this.term);

        if (log.isTraceEnabled()) {
            log.trace("EXIT become_leader");
        }
    }


    /// Resets the current node to a given term.
    public void reset(long term) {
        if (this.term != term) {
            this.term = term;
            this.vote = INVALID_ID;
        }

        this.leaderId = INVALID_ID;
        this.resetRandomizedElectionTimeout();
        this.electionElapsed = 0;
        this.heartbeatElapsed = 0;

        this.abortLeaderTransfer();

        this.votes.clear();

        this.pendingConfIndex = 0;
        this.readOnly = new ReadOnly(this.readOnly.getOption());
        this.pendingRequestSnapshot = INVALID_ID;

        var lastIndex = this.raftLog.lastIndex();
        var selfId = this.id;
        this.prs.getProgress().forEach((id, pr) -> {
            pr.reset(lastIndex + 1);
            if (id == selfId) {
                pr.setMatched(lastIndex);
            }
        });
    }

    /// Regenerates and stores the election timeout.
    public void resetRandomizedElectionTimeout() {
        var timeout = new Random().nextInt(this.maxElectionTimeout - this.minElectionTimeout + 1) + this.minElectionTimeout;

        if (log.isDebugEnabled()) {
            log.debug("reset election timeout {} -> {} at {}",
                    this.randomizedElectionTimeout,
                    timeout,
                    this.electionElapsed);
        }

        this.randomizedElectionTimeout = timeout;
    }


    /// Appends a slice of entries to the log. The entries are updated to match
    /// the current index and term.
    public void appendEntry(List<Eraftpb.Entry.Builder> entries) {
        var lastIndex = this.raftLog.lastIndex();

        var i = new AtomicInteger();
        var appendEntries = entries.stream()
                .map(s -> s.setTerm(this.term).setIndex(lastIndex + 1 + i.getAndIncrement()))
                .collect(Collectors.toList());

        var appendAfterIndex = this.raftLog.append(appendEntries);

        this.prs.get(this.id).maybeUpdate(appendAfterIndex);

        // Regardless of maybe_commit's return, our caller will call bcastAppend.
        this.maybeCommit();
    }

    /// Attempts to advance the commit index. Returns true if the commit index
    /// changed (in which case the caller should call `r.bcast_append`).
    private boolean maybeCommit() {
        var mci = this.prs.maximalCommittedIndex(this.quorumFunction);
        return this.raftLog.maybeCommit(mci, this.term);
    }

    private void hup(boolean transferLeader) {
        if (this.state == StateRole.Leader) {
            if (log.isDebugEnabled()) {
                log.debug("ignoring MsgHup because already leader");
            }
            return;
        }
        // If there is a pending snapshot, its index will be returned by
        // `maybe_first_index`. Note that snapshot updates configuration
        // already, so as long as pending entries don't contain conf change
        // it's safe to start campaign.
        var firstIndex = Optional.ofNullable(this.raftLog.getUnstable().maybeFirstIndex())
                .orElseGet(() -> this.raftLog.getApplied() + 1);
        var lastIndex = this.raftLog.getCommitted() + 1;

        List<Eraftpb.Entry.Builder> entries;
        try {
            entries = this.raftLog.slice(firstIndex, lastIndex, null);
        } catch (RaftErrorException e) {
            throw new PanicException(log, "unexpected error getting unApplied entries [{}, {}]",
                    firstIndex,
                    lastIndex);
        }

        var pendingConf = this.numPendingConf(entries);
        if (pendingConf != 0) {
            log.warn("cannot campaign at term {} since there are still {} pending configuration changes to apply",
                    this.term,
                    pendingConf);
            return;
        }

        log.info("starting a new election; term {}", this.term);

        if (transferLeader) {
            this.campaign(CAMPAIGN_TRANSFER);
        } else if (this.preVote) {
            this.campaign(CAMPAIGN_PRE_ELECTION);
        } else {
            this.campaign(CAMPAIGN_ELECTION);
        }
    }

    /// Campaign to attempt to become a leader.
    ///
    /// If pre vote is enabled, this is handled as well.
    private void campaign(ByteString campaignType) {

        Eraftpb.MessageType voteMsgType;
        long term;
        if (CAMPAIGN_PRE_ELECTION.equals(campaignType)) {
            this.becomePreCandidate();
            voteMsgType = MsgRequestPreVote;
            term = this.term + 1;
        } else {
            this.becomeCandidate();
            voteMsgType = MsgRequestVote;
            term = this.term;
        }

        var selfId = this.id;

        this.registerVote(selfId, true);

        if (CandidacyStatus.Elected == this.prs.candidacyStatus(this.votes, this.quorumFunction)) {
            // We won the election after voting for ourselves (which must mean that
            // this is a single-node cluster). Advance to the next state.
            if (CAMPAIGN_PRE_ELECTION.equals(campaignType)) {
                this.campaign(CAMPAIGN_ELECTION);
            } else {
                this.becomeLeader();
            }
            return;
        }

        this.prs.voterIds().stream().filter(id -> id != selfId)
                .forEach(id -> {
                    var lastTerm = this.raftLog.lastTerm();
                    var lastIndex = this.raftLog.lastIndex();

                    log.info("[log term: {}, index: {}] sent request to {} term {}, msgType {}",
                            lastTerm,
                            lastIndex,
                            id,
                            this.term,
                            voteMsgType);

                    var msgBuilder = newMessage(id, voteMsgType, null)
                            .setTerm(term)
                            .setIndex(lastIndex)
                            .setLogTerm(lastTerm);

                    if (CAMPAIGN_TRANSFER.equals(campaignType)) {
                        msgBuilder.setContext(campaignType);
                    }

                    this.send(msgBuilder);
                });
    }

    /// Sets the vote of `id` to `vote`.
    private void registerVote(long id, boolean vote) {
        this.votes.putIfAbsent(id, vote);
    }

    private long numPendingConf(List<Eraftpb.Entry.Builder> entries) {
        return entries.stream().filter(s -> s.getEntryType() == Eraftpb.EntryType.EntryConfChange).count();
    }

    private void logVoteApprove(Eraftpb.Message.Builder message) {
        log.info("[log term: {}, index: {}, vote: {}] cast vote for {} [log term: {}, index: {}] at term {} msgType {}",
                this.raftLog.lastTerm(),
                this.raftLog.lastIndex(),
                this.vote,
                message.getFrom(),
                message.getLogTerm(),
                message.getIndex(),
                this.term,
                message.getMsgType());
    }

    private void logVoteReject(Eraftpb.Message.Builder message) {
        log.info("[log term: {}, index: {}, vote: {}] rejected vote for {} [log term: {}, index: {}] at term {} msgType {}",
                this.raftLog.lastTerm(),
                this.raftLog.lastIndex(),
                this.vote,
                message.getFrom(),
                message.getLogTerm(),
                message.getIndex(),
                this.term,
                message.getMsgType());
    }

    private void handleAppendEntries(Eraftpb.Message.Builder m) {
        if (this.pendingRequestSnapshot == INVALID_INDEX) {
            this.sendRequestSnapshot();
            return;
        }

        if (m.getIndex() < this.raftLog.getCommitted()) {
            if (log.isDebugEnabled()) {
                log.debug("got message with lower index than committed.");
            }

            var toSend = Eraftpb.Message.newBuilder().setMsgType(MsgAppendResponse)
                    .setTo(m.getFrom())
                    .setIndex(this.raftLog.getCommitted());
            this.send(toSend);
            return;
        }

        var toSend = Eraftpb.Message.newBuilder().setMsgType(MsgAppendResponse)
                .setTo(m.getFrom())
                .setIndex(this.raftLog.getCommitted());

        var lastIdx = this.raftLog.maybeAppend(m.getIndex(), m.getLogTerm(), m.getCommit(), m.getEntriesBuilderList());

        if (lastIdx != null) {
            toSend.setIndex(lastIdx);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("rejected msgApp [log term: {}, index: {}] from {}",
                        m.getLogTerm(),
                        m.getIndex(),
                        m.getFrom());
            }
            toSend.setIndex(m.getIndex())
                    .setReject(true)
                    .setRejectHint(this.raftLog.lastIndex());
        }
        this.send(toSend);
    }

    private void sendRequestSnapshot() {
        var toSend = Eraftpb.Message.newBuilder().setMsgType(MsgAppendResponse)
                .setIndex(this.raftLog.getCommitted())
                .setReject(true)
                .setRejectHint(this.raftLog.lastIndex())
                .setTo(this.leaderId).setRequestSnapshot(this.pendingRequestSnapshot);
        this.send(toSend);
    }

    /// For a message, commit and send out heartbeat.
    private void handleHeartbeat(Eraftpb.Message.Builder m) {
        this.raftLog.commitTo(m.getCommit());
        if (this.pendingRequestSnapshot != INVALID_INDEX) {
            this.sendRequestSnapshot();
            return;
        }
        var toSend = Eraftpb.Message.newBuilder()
                .setMsgType(MsgHeartbeatResponse)
                .setTo(m.getFrom())
                .setContext(m.getContext());

        this.send(toSend);
    }

    private void handleSnapshot(Eraftpb.Message m) {
        var meta = m.getSnapshot().getMetadata();
        if (this.restore(m.getSnapshot())) {
            log.info("[commit: {}, term: {}] restored snapshot [index: {}, term: {}]",
                    this.term,
                    this.raftLog.getCommitted(),
                    meta.getIndex(),
                    meta.getTerm());

            var toSend = Eraftpb.Message.newBuilder().setMsgType(MsgAppendResponse)
                    .setTo(m.getFrom())
                    .setIndex(this.raftLog.lastIndex());
            this.send(toSend);
        } else {
            log.info("[commit: {}, term: {}] ignored snapshot [index: {}, term: {}]",
                    this.term,
                    this.raftLog.getCommitted(),
                    meta.getIndex(),
                    meta.getTerm());

            var toSend = Eraftpb.Message.newBuilder()
                    .setMsgType(MsgAppendResponse)
                    .setTo(m.getFrom())
                    .setIndex(this.raftLog.getCommitted());
            this.send(toSend);
        }
    }

    /// Recovers the state machine from a snapshot. It restores the log and the
    /// configuration of state machine.
    public boolean restore(Eraftpb.Snapshot snapshot) {
        if (snapshot.getMetadata().getIndex() < this.raftLog.getCommitted()) {
            return false;
        }

        var result = this.restoreRaft(snapshot);
        if (result != null) {
            return result;
        }
        this.raftLog.restore(snapshot);
        return true;
    }

    private Boolean restoreRaft(Eraftpb.Snapshot snapshot) {
        var meta = snapshot.getMetadata();
        // Do not fast-forward commit if we are requesting snapshot.
        if (this.pendingRequestSnapshot == INVALID_INDEX && this.raftLog.matchTerm(meta.getIndex(), meta.getTerm())) {
            log.info("[commit: {}, lastIndex: {}, lastTerm: {}] " +
                            "fast-forwarded commit to snapshot [index: {}, term: {}]",
                    this.raftLog.getCommitted(),
                    this.raftLog.lastIndex(),
                    this.raftLog.lastTerm(),
                    meta.getIndex(),
                    meta.getTerm());
            this.raftLog.commitTo(meta.getIndex());
            return false;
        }

        // After the Raft is initialized, a voter can't become a learner any more.
        if (this.prs.getProgress().size() != 0 && this.promotable) {
            for (Long id : meta.getConfState().getLearnersList()) {
                if (id == this.id) {
                    log.error("can't become learner when restores snapshot, snapshot index {}, snapshot term {}",
                            meta.getIndex(),
                            meta.getTerm());
                    return false;
                }
            }
        }

        log.info("[commit: {}, lastIndex: {}, lastTerm: {}] starts to restore snapshot [index: {}, term: {}]",
                this.raftLog.getCommitted(),
                this.raftLog.lastIndex(),
                this.raftLog.lastTerm(),
                meta.getIndex(),
                meta.getTerm());

        // Restore progress set and the learner flag.
        var nextIdx = this.raftLog.lastIndex() + 1;
        this.prs.restoreSnapMeta(meta, nextIdx, this.maxInflight);
        this.prs.get(this.id).setMatched(nextIdx - 1);
        if (this.prs.getConfiguration().getVoters().contains(this.id)) {
            this.promotable = true;
        } else if (this.prs.getConfiguration().getLearners().contains(this.id)) {
            this.promotable = false;
        }

        this.pendingRequestSnapshot = INVALID_INDEX;
        return null;
    }

    /// Sends RPC, with entries to all peers that are not up-to-date
    /// according to the progress recorded in r.prs().
    private void bcastAppend() {
        this.prs.getProgress().entrySet()
                .stream()
                .filter(e -> e.getKey() != this.id)
                .forEach(e -> this.sendAppend(e.getKey(), e.getValue()));
    }

    /// Sends RPC, without entries to all the peers.
    private void bcastHeartbeat() {
        var ctx = this.readOnly.lastPendingRequestCtx();
        this.bcastHeartbeatWithCtx(ctx);
    }

    private void bcastHeartbeatWithCtx(ByteString ctx) {
        this.prs.getProgress().forEach((id, pr) -> {
            if (id != this.id) {
                this.sendHeartbeat(id, pr, ctx);
            }
        });
    }

    // check_quorum_active returns true if the quorum is active from
    // the view of the local raft state machine. Otherwise, it returns
    // false.
    // check_quorum_active also resets all recent_active to false.
    // check_quorum_active can only called by leader.
    private boolean checkQuorumActive() {
        return this.prs.quorumRecentlyActive(this.id, this.quorumFunction);
    }

    /// Check if there is any pending conf change.
    ///
    /// This method can be false positive.
    public boolean hasPendingConf() {
        return this.pendingConfIndex > this.raftLog.getApplied();
    }

    public Eraftpb.HardState hardState() {
        return Eraftpb.HardState.newBuilder()
                .setTerm(this.term)
                .setVote(this.vote)
                .setCommit(this.raftLog.getCommitted())
                .build();
    }

    public SoftState softState() {
        return new SoftState(this.leaderId, this.state);
    }

    /// Issues a message to timeout immediately.
    private void sendTimeoutNow(long to) {
        var toSend = newMessage(to, MsgTimeoutNow, null);
        this.send(toSend);
    }

    /// Specifies if the commit should be broadcast.
    private boolean  shouldBcastCommit() {
        return !this.skipBCastCommit || this.hasPendingConf();
    }

    public void commitApply(long applied) {
        this.getRaftLog().appliedTo(applied);
    }

    public void ping() {
        if (this.state == StateRole.Leader) {
            this.bcastHeartbeat();
        }
    }

    public void addNode(long id) throws RaftErrorException {
        this.addVoterOrLearner(id, false);
    }

    private void addVoterOrLearner(long id, boolean learner) throws RaftErrorException {
        log.debug("adding node (learner: {}) with ID {} to peers.", learner, id);

        if (learner) {
            this.getPrs().insertLearner(id, new Progress(this.getRaftLog().lastIndex() + 1, this.maxInflight));
        } else if (this.prs.learnerIds().contains(id)) {
            this.getPrs().promoteLearner(id);
        } else {
            this.getPrs().insertVoter(id, new Progress(this.getRaftLog().lastIndex() + 1, this.maxInflight));
        }

        if (this.id == id) {
            this.promotable = !learner;
        }
        // When a node is first added/promoted, we should mark it as recently active.
        // Otherwise, check_quorum may cause us to step down if it is invoked
        // before the added node has a chance to communicate with us.
        this.getPrs().get(id).setRecentActive(true);
    }

    public void addLearner(long id) throws RaftErrorException {
        this.addVoterOrLearner(id, true);
    }

    public void removeNode(long id) {
        this.getPrs().remove(id);

        // do not try to commit or abort transferring if there are no voters in the cluster.
        if (this.prs.voterIds().isEmpty()) {
            return;
        }

        // The quorum size is now smaller, so see if any pending entries can
        // be committed.
        if (this.maybeCommit()) {
            this.bcastAppend();
        }
        // If the removed node is the lead_transferee, then abort the leadership transferring.
        if (this.state == StateRole.Leader && this.leadTransferee == id) {
            this.abortLeaderTransfer();
        }
    }

    public Eraftpb.Snapshot snap() {
        return this.getRaftLog().getUnstable().getSnapshot();
    }

    public void requestSnapshot(long requestIndex) throws RaftErrorException {
        if (this.state == StateRole.Leader) {
            log.info("can not request snapshot on leader; dropping request snapshot");
        } else if (this.leaderId == INVALID_ID) {
            log.info("drop request snapshot because of no leader");
        } else if (this.snap() != null) {
            log.info("there is a pending snapshot; dropping request snapshot");
        } else if (this.pendingRequestSnapshot != INVALID_INDEX) {
            log.info("there is a pending snapshot; dropping request snapshot");
        } else {
            this.pendingRequestSnapshot = requestIndex;
            this.sendRequestSnapshot();
            return;
        }
        throw new RaftErrorException(RaftError.RequestSnapshotDropped);
    }

    public Storage store() {
        return this.getRaftLog().getStore();
    }

    public Storage mutStore() {
        return this.getRaftLog().getStore();
    }

    /// For a given hardState, load the state into self.
    private void loadState(Eraftpb.HardState.Builder hs) {
        if (hs.getCommit() < this.raftLog.getCommitted() || hs.getCommit() > this.raftLog.lastIndex()) {
            throw new PanicException(log, "hs.commit {} is out of range [{}, {}]",
                    hs.getCommit(),
                    this.raftLog.getCommitted(),
                    this.raftLog.lastIndex());
        }
        this.raftLog.setCommitted(hs.getCommit());
        this.term = hs.getTerm();
        this.vote = hs.getVote();
    }

}
