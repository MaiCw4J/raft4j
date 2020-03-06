package com.stephen;

import eraftpb.Eraftpb;
import lombok.extern.slf4j.Slf4j;

/**
 * RawNode is a thread-unsafe Node.
 * The methods of this struct correspond to the methods of Node and are described
 * more fully there.
 */
@Slf4j
public class RawNode {

    private Raft raft;

    private Eraftpb.HardState prevHs;

    private Eraftpb.HardState prevSs;


    public RawNode(Config config, Storage store) {
        assert !(config.getId() == 0);
        Raft raft = new Raft();
    }
}
