package com.stephen;

import com.stephen.constanst.StateRole;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@AllArgsConstructor
@EqualsAndHashCode
public class SoftState {

    private long leaderId;

    private StateRole raftState;

    public SoftState() {
        this(0,StateRole.Follower);
    }
}
