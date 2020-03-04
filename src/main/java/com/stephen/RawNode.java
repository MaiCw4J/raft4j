package com.stephen;

import eraftpb.Eraftpb;

public class RawNode {

    private Raft raft;

    private Eraftpb.HardState prevHs;

    private Eraftpb.HardState prevSs;

}
