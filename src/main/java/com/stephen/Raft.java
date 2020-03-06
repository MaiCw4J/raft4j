package com.stephen;

import com.stephen.constanst.StateRole;
import com.stephen.exception.PanicException;
import com.stephen.progress.ProgressSet;
import com.stephen.raft.SoftState;
import eraftpb.Eraftpb;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static eraftpb.Eraftpb.MessageType.*;

@Slf4j
@Data
public class Raft {

    /// The current election term.
//    pub term: u64,
    private long term;

    /// Which peer this raft is voting for.
    private long vote;

    /// The ID of this node.
    private long id;

    /// The current read states.
//    pub read_states: Vec<ReadState>,
//    private List<ReadState> readStates;

    /// The persistent log.
    private RaftLog raftLog;

    /// The maximum number of messages that can be inflight.
    private long maxInflight;

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
    private List<Eraftpb.Message> msgs;

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
//    pub read_only: ReadOnly,

    /// Ticks since it reached last electionTimeout when it is leader or candidate.
    /// Number of ticks since it reached last electionTimeout or received a
    /// valid message from current leader when it is a follower.
    private long electionElapsed;


    /// Number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    private long heartbeatElapsed;


    /// Whether to check the quorum
    private boolean checkQuorum;

    /// Enable the prevote algorithm.
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
    private long randomizedElectionTimeout;
    private long min_electionTimeout;
    private long max_electionTimeout;

    private QuorumFunction quorumFunction;


    private Eraftpb.Message newMessage(long to, Eraftpb.MessageType fieldType, Long from) {
        var builder = Eraftpb.Message.newBuilder()
                .setTo(to)
                .setMsgType(fieldType);
        if (from != null) {
            builder.setFrom(from);
        }
        return builder.build();
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
    public void send(Eraftpb.Message message) {

        var type = message.getMsgType();

        switch (type) {
            case MsgRequestVote, MsgRequestPreVote, MsgRequestVoteResponse, MsgRequestPreVoteResponse -> {
                if (message.getTerm() == 0) {
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
                var messageTerm = message.getTerm();
                if (messageTerm != 0) {
                    throw new PanicException(log, "term should not be set when sending {:?} (was {})", type.name(), messageTerm);
                }

                // do not attach term to MsgPropose, MsgReadIndex
                // proposals are a way to forward to the leader and
                // should be treated as local message.
                // MsgReadIndex is also forwarded to leader.
                if (type != MsgPropose && type != MsgReadIndex) {
//                    m.term = self.term;
                }

            }
        }
        this.msgs.add(message);
    }

    public Eraftpb.HardState hardState() {
        return Eraftpb.HardState.getDefaultInstance()
                .toBuilder()
                .setTerm(this.term)
                .setVote(this.vote)
                .setCommit(this.raftLog.getCommitted())
                .build();

    }

    public SoftState softState() {
        return new SoftState(this.leaderId, this.state);
    }
}
