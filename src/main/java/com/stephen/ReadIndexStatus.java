package com.stephen;

import eraftpb.Eraftpb;

import java.util.Set;

public class ReadIndexStatus {

    private Eraftpb.Message req;
    private long index;
    private Set<Long> acks;

}
