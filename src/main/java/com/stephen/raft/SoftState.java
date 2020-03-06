package com.stephen.raft;

import com.stephen.constanst.StateRole;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SoftState {

    private long leaderId;

    private StateRole raftState;

}
