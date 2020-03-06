package com.stephen;

import com.stephen.raft.Ready;
import com.stephen.raft.SoftState;
import eraftpb.Eraftpb;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * RawNode is a thread-unsafe Node.
 * The methods of this struct correspond to the methods of Node and are described
 * more fully there.
 */
@Slf4j
@Data
@AllArgsConstructor
public class RawNode {

    private Raft raft;

    private Eraftpb.HardState prevHs;

    private SoftState prevSs;

    public RawNode(Config config, Storage store) {
        assert !(config.getId() == 0);
        Raft r = new Raft();
        RawNode rn = new RawNode(r, Eraftpb.HardState.getDefaultInstance(), new SoftState());
        rn.setPrevHs(rn.getRaft().hardState());
        rn.setPrevSs(rn.getRaft().softState());
        log.info("RawNode created with id {}.", rn.getRaft().getId());
    }

    public static RawNode withDefaultLogger(Config c, Storage store) {
        return new RawNode(c, store);
    }
    
}
