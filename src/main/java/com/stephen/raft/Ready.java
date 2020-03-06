package com.stephen.raft;

import com.stephen.ReadState;
import com.stephen.lang.Vec;
import eraftpb.Eraftpb;
import lombok.Data;

@Data
public class Ready {

    private SoftState ss;

    private Eraftpb.HardState hs;

    private Vec<ReadState> readStates;

    private Vec<Eraftpb.Entry> entries;
    
    private Eraftpb.Snapshot snapshot;

    private Vec<Eraftpb.Entry> committedEntries;

}
