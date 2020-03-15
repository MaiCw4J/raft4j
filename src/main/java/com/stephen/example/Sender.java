package com.stephen.example;

import eraftpb.Eraftpb;
import lombok.AllArgsConstructor;

import java.util.concurrent.LinkedBlockingQueue;


@AllArgsConstructor
public class Sender {
    private LinkedBlockingQueue<Eraftpb.Message> channel;

    public void send(Eraftpb.Message message) {
        channel.offer(message);
    }

}
