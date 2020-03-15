package com.stephen;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.stephen.constanst.ReadOnlyOption;
import com.stephen.exception.PanicException;
import com.stephen.lang.Vec;
import eraftpb.Eraftpb;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Stream;

@Slf4j
public class ReadOnly {

    @Getter
    private ReadOnlyOption option;
    private Map<ByteString, ReadIndexStatus> pendingReadIndex;
    private Deque<ByteString> readIndexQueue;

    public ReadOnly(ReadOnlyOption option) {
        this.option = option;
        this.pendingReadIndex = new HashMap<>();
        this.readIndexQueue = new ArrayDeque<>();
    }

    /// Adds a read only request into readonly struct.
    ///
    /// `index` is the commit index of the raft state machine when it received
    /// the read only request.
    ///
    /// `m` is the original read only request message from the local or remote node.
    public void addRequest(long index, Eraftpb.Message m) {
        var ctx = m.getEntries(0).getData();
        if (this.pendingReadIndex.containsKey(ctx)) {
            return;
        }

        var status = new ReadIndexStatus(m, index, new HashSet<>());

        this.pendingReadIndex.put(ctx, status);
        this.readIndexQueue.addLast(ctx);
    }

    /// Notifies the ReadOnly struct that the raft state machine received
    /// an acknowledgment of the heartbeat that attached with the read only request
    /// context.
    public Set<Long> recvAck(Eraftpb.Message.Builder m) {
        var rs = this.pendingReadIndex.get(m.getContext());
        if (rs != null) {
            rs.getAcks().add(m.getFrom());
            // add one to include an ack from local node

            var setWithSelf = new HashSet<>(rs.getAcks());
            setWithSelf.add(m.getTo());
            return setWithSelf;
        } else {
            return new HashSet<>();
        }
    }

    /// Advances the read only request queue kept by the ReadOnly struct.
    /// It de queues the requests until it finds the read only request that has
    /// the same context as the given `m`.
    public List<ReadIndexStatus> advance(ByteString ctx) {
        List<ReadIndexStatus> rss = new ArrayList<>();
        ByteString bytes;
        while ((bytes = this.readIndexQueue.pollFirst()) != null) {
            var ris = this.pendingReadIndex.remove(bytes);
            if (ris == null) {
                throw new PanicException(log, "cannot find correspond read state from pending map");
            }
            rss.add(ris);
            if (Objects.equals(bytes, ctx)) {
                break;
            }
        }
        return rss;
    }


    /// Returns the context of the last pending read only request in ReadOnly struct.
    public ByteString lastPendingRequestCtx() {
        return this.readIndexQueue.peekLast();
    }


    public int pendingReadCount() {
        return this.readIndexQueue.size();
    }

}
