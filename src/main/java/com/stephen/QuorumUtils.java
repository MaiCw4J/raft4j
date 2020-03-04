package com.stephen;

public class QuorumUtils {

    public static int calculateQuorum(QuorumFunction quorumFunction, int votersSize) {

        var quorum = quorumFunction.apply(votersSize);
        if (quorumFunction != (QuorumFunction) QuorumUtils::majority) {
            quorum = Math.min(Math.max(quorum, majority(votersSize)), votersSize);
        }

        return quorum;
    }

    /// Get the majority number of given nodes count.
    public static int majority(int total) {
        return (total / 2) + 1;
    }

}
