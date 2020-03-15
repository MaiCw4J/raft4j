package com.stephen.example;

import eraftpb.Eraftpb;
import lombok.AllArgsConstructor;

import java.util.concurrent.LinkedBlockingQueue;

@AllArgsConstructor
public class Receiver {
    private LinkedBlockingQueue<Eraftpb.Message> channel;

    public Eraftpb.Message tryRecv() {
        return channel.poll();
    }
}
