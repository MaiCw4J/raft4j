package com.stephen;

import com.stephen.constanst.Globals;
import com.stephen.constanst.ReadOnlyOption;
import com.stephen.exception.RaftError;
import com.stephen.exception.RaftErrorException;
import lombok.Data;

@Data
public class Config {

    /**
     * The identity of the local raft. It cannot be 0, and must be unique in the group.
     * 本地raft唯一id
     */
    private long id;

    /**
     * The number of node.tick invocations that must pass between
     * elections. That is, if a follower does not receive any message from the
     * leader of current term before ElectionTick has elapsed, it will become
     * candidate and start an election. election_tick must be greater than
     * HeartbeatTick. We suggest election_tick = 10 * HeartbeatTick to avoid
     * unnecessary leader switching
     * <p>
     * 多久没收到leader的心跳，该节点成为候选者
     */
    private long electionTick;

    /**
     * HeartbeatTick is the number of node.tick invocations that must pass between
     * heartbeats. That is, a leader sends heartbeat messages to maintain its
     * leadership every heartbeat ticks.
     * <p>
     * leader节点心跳时间
     */
    private long heartbeatTick;

    /**
     * Applied is the last applied index. It should only be set when restarting
     * raft. raft will not return entries to the application smaller or equal to Applied.
     * If Applied is unset when restarting, raft might return previous applied entries.
     * This is a very application dependent configuration.
     * <p>
     * 最新的索引index
     */
    private long applied;

    /**
     * Limit the max number of in-flight append messages during optimistic
     * replication phase. The application transportation layer usually has its own sending
     * buffer over TCP/UDP. Set to avoid overflowing that sending buffer.
     */
    private long maxSizePerMsg;

    /**
     * Limit the max number of in-flight append messages during optimistic
     * replication phase. The application transportation layer usually has its own sending
     * buffer over TCP/UDP. Set to avoid overflowing that sending buffer.
     */
    private long maxInflightMsgs;

    /**
     * Specify if the leader should check quorum activity. Leader steps down when
     * quorum is not active for an electionTimeout.
     */
    private boolean checkQuorum;

    /**
     * Enables the Pre-Vote algorithm described in raft thesis section
     * 9.6. This prevents disruption when a node that has been partitioned away
     * rejoins the cluster.
     * <p>
     * 是否启用预投票算法
     */
    private boolean preVote;

    /**
     * The range of election timeout. In some cases, we hope some nodes has less possibility
     * to become leader. This configuration ensures that the randomized election_timeout
     * will always be suit in [min_election_tick, max_election_tick).
     * If it is 0, then election_tick will be chosen.
     */
    private long minElectionTick;

    /**
     * If it is 0, then 2 * election_tick will be chosen.
     */
    private long maxElectionTick;

    /**
     * Choose the linearizability mode or the lease mode to read data. If you don’t care about the read consistency and want a higher read performance, you can use the lease mode.
     * Setting this to `LeaseBased` requires `check_quorum = true`.
     */
    private ReadOnlyOption readOnlyOption;

    /**
     * Don't broadcast an empty raft entry to notify follower to commit an entry.
     * This may make follower wait a longer time to apply an entry. This configuration
     * May affect proposal forwarding and follower read.
     */
    private boolean skipBcastCommit;

    /**
     * Batches every append msg if any append msg already exists
     */
    private boolean batchAppend;

    public Config() {
        long HEARTBEAT_TICK = 2;
        this.id = Globals.INVALID_ID;
        this.electionTick = HEARTBEAT_TICK * 10;
        this.heartbeatTick = HEARTBEAT_TICK;
        this.applied = 0;
        this.maxSizePerMsg = 0;
        this.maxInflightMsgs = 256;
        this.checkQuorum = false;
        this.preVote = false;
        this.minElectionTick = 0;
        this.maxElectionTick = 0;
        this.readOnlyOption = ReadOnlyOption.Safe;
        this.skipBcastCommit = false;
        this.batchAppend = false;
    }

    public Config(long id) {
        this();
        this.id = id;
    }

    /**
     * The minimum number of ticks before an election.
     */
    public long minElectionTick() {
        return this.minElectionTick == 0 ? this.electionTick : this.minElectionTick;
    }

    /**
     * The maximum number of ticks before an election.
     */
    public long maxElectionTick() {
        return this.maxElectionTick == 0 ? 2 * this.electionTick : this.maxElectionTick;
    }

    /**
     * Runs validations against the config.
     */
    public void validate() throws RaftErrorException {
        if (this.id == Globals.INVALID_ID) {
            throw new RaftErrorException(RaftError.ConfigInvalid, "invalid node id");
        }

        if (this.heartbeatTick == 0) {
            throw new RaftErrorException(RaftError.ConfigInvalid,
                    "heartbeat tick must greater than 0");
        }

        if (this.electionTick <= this.heartbeatTick) {
            throw new RaftErrorException(RaftError.ConfigInvalid,
                    "election tick must be greater than heartbeat tick");
        }

        long minTimeout = this.minElectionTick();
        long maxTimeout = this.maxElectionTick();
        if (minTimeout < this.electionTick) {
            throw new RaftErrorException(RaftError.ConfigInvalid,
                    String.format("min election tick %d must not be less than election_tick %d",
                            minTimeout, this.electionTick));
        }

        if (minTimeout >= maxTimeout) {
            throw new RaftErrorException(RaftError.ConfigInvalid,
                    String.format("min election tick %d should be less than max election tick %d",
                            minTimeout, maxTimeout));
        }

        if (this.maxInflightMsgs == 0) {
            throw new RaftErrorException(RaftError.ConfigInvalid,
                    "max inflight messages must be greater than 0");
        }

        if (this.readOnlyOption == ReadOnlyOption.LeaseBased && !this.checkQuorum) {
            throw new RaftErrorException(RaftError.ConfigInvalid,
                    "read_only_option == LeaseBased requires check_quorum == true");
        }
    }
}
