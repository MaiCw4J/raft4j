package com.stephen;

import eraftpb.Eraftpb;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Set;

@Getter
@AllArgsConstructor
public class ReadIndexStatus {

    private Eraftpb.Message req;
    private long index;
    private Set<Long> acks;

}
