package com.stephen.example;

import com.google.protobuf.ByteString;
import com.stephen.Config;
import com.stephen.RawNode;
import com.stephen.constanst.StateRole;
import com.stephen.exception.PanicException;
import com.stephen.exception.RaftErrorException;
import com.stephen.storage.MemStorage;
import eraftpb.Eraftpb;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static eraftpb.Eraftpb.MessageType.*;

@SuppressWarnings("SwitchStatementWithTooFewBranches")
public class FiveNode {

    private static final int NODE_NUM = 5;

    private static final Pattern REG = Pattern.compile("put ([0-9]+) (.+)");

    private static final AtomicBoolean STOP_SIGNAL = new AtomicBoolean(false);

    public static void main(String[] args) throws RaftErrorException, InterruptedException {

        Thread[] threads = new Thread[6];
        LinkedBlockingQueue<Proposal> proposals = new LinkedBlockingQueue<>();

        Map<Long, Sender> mailboxes = new HashMap<>();
        for (int i = 1; i <= NODE_NUM; i++) {
            var receiver = transport(i, mailboxes);

            var node = switch (i) {
                case 1 -> createRaftLeader(i, mailboxes, receiver);
                default -> createRaftFollower(mailboxes, receiver);
            };

            int finalI = i;
            Runnable runnable = () -> {

                var t = System.currentTimeMillis();

                for (;;) {
                    for (;;) {
                        var msg = node.receiver.tryRecv();
                        if (msg != null) {
                            System.out.println("node " + finalI + "got message");
                            node.step(msg);
                        } else {
                            break;
                        }
                    }

                    var raftGroup = node.raftGroup;
                    if (raftGroup == null) {
                        // When Node::raft_group is `None` it means the node is not initialized.
                        continue;
                    }

                    if (System.currentTimeMillis() - t > 300) {
                        raftGroup.tick();
                        t = System.currentTimeMillis();
                    }

                    // Let the leader pick pending proposals from the global queue.
                    if (raftGroup.getRaft().getState() == StateRole.Leader) {
                        // Handle new proposals.
                        for (Proposal proposal : proposals) {
                            if (proposal.proposed == 0) {
                                propose(raftGroup, proposal);
                            }
                        }
                    }

                    // Handle readies from the raft.
                    onReady(raftGroup, node, proposals);

                    // Check control signals from
                    if (STOP_SIGNAL.get()) {
                        break;
                    }
                }
            };
            threads[i] = new Thread(runnable);
        }

        for (int i = 1; i <= NODE_NUM; i++) {
            threads[i].start();
        }

        addAllFollowers(proposals);

        for (int i = 1; i <= 100; i++) {
            Proposal proposal = new Proposal(i, "hello, world" + i);
            proposals.offer(proposal);
            // After we got a response from `rx`, we can assume the put succeeded and following
            // `get` operations can find the key-value pair.
            proposal.await();
        }

        STOP_SIGNAL.set(true);

        // await all thread done!
        for (int i = 1; i <= NODE_NUM; i++) {
            threads[i].join();
        }

    }

    private static void onReady(RawNode<MemStorage> raftGroup, Node node, LinkedBlockingQueue<Proposal> proposals) {
        if (!raftGroup.hasReady()) {
            return;
        }

        // Get the `Ready` with `RawNode::ready` interface.
        var ready = raftGroup.ready();

        MemStorage store = raftGroup.getRaft().getRaftLog().getStore();

        // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        store.wl(s -> s.append(ready.getEntries()));

        // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
        var s = ready.getSnapshot();
        if (!Objects.equals(s, Eraftpb.Snapshot.getDefaultInstance())) {
            store.wl(core -> core.applySnapshot(s));
        }

        // Send out the messages come from the node.

        ready.getMessages().forEach(m -> node.mailboxes.get(m.getTo()).send(m));

        // Apply all committed proposals.
        var committedEntries = ready.getCommittedEntries();
        if (committedEntries != null && !committedEntries.isEmpty()) {
            for (Eraftpb.Entry entry : committedEntries) {
                if (entry.getData().isEmpty()) {
                    // From new elected leaders.
                    continue;
                }

                if (entry.getEntryType() == Eraftpb.EntryType.EntryConfChange) {
                    // For conf change messages, make them effective.
                    try {
                        var cs = raftGroup.applyConfChange(Eraftpb.ConfChange.parseFrom(entry.getData()));
                        store.wl(core -> core.setConfState(cs));
                    } catch (Exception ignored) {
                    }
                } else {
                    // For normal proposals, extract the key-value pair and then
                    // insert them into the kv engine.
                    var data = entry.getData().toString();
                    Matcher matcher = REG.matcher(data);
                    if (matcher.find()) {
                        node.kv.put(Long.valueOf(matcher.group(1)), matcher.group(2));
                    }
                }
                if (raftGroup.getRaft().getState() == StateRole.Leader) {
                    // The leader should response to the clients, tell them if their proposals
                    // succeeded or not.
                    var proposal = proposals.poll();
                    if (proposal != null) {
                        proposal.done(true);
                    }
                }
            }

            committedEntries.last().ifPresent(last -> store.wl(core -> core.mutHardState()
                    .setCommit(last.getIndex())
                    .setTerm(last.getTerm())));
        }

        // Call `RawNode::advance` interface to update position flags in the raft.
        raftGroup.advance(ready);
    }

    // Proposes some conf change for peers [2, 5].
    private static void addAllFollowers(LinkedBlockingQueue<Proposal> proposals) {
        for (int i = 2; i <= 5; i++) {
            System.out.println("start add node " + i);
            var cc = Eraftpb.ConfChange.newBuilder().setNodeId(i)
                    .setChangeType(Eraftpb.ConfChangeType.AddNode)
                    .build();
            Proposal proposal = new Proposal(cc);
            proposals.offer(proposal);

            proposal.await();
            System.out.println("add node done " + i);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class Node {
        // None if the raft is not initialized.
        private RawNode<MemStorage> raftGroup;
        private Receiver receiver;
        private Map<Long, Sender> mailboxes;
        // Key-value pairs after applied. `MemStorage` only contains raft logs,
        // so we need an additional storage engine.
        private Map<Long, String> kv;

        // Step a raft message, initialize the raft if need.
        public void step(Eraftpb.Message msg) {
            try {
                if (this.raftGroup == null) {
                    if (isInitialMsg(msg)) {
                        this.initializeRaftFromMessage(msg);
                    } else {
                        return;
                    }
                }
                this.raftGroup.step(msg);
            } catch (RaftErrorException e) {
                throw new PanicException(e);
            }
        }

        // Initialize raft for followers.
        private void initializeRaftFromMessage(Eraftpb.Message msg) throws RaftErrorException {
            if (!isInitialMsg(msg)) {
                return;
            }
            var c = exampleConfig(msg.getTo());
            var storage = new MemStorage();
            this.raftGroup = new RawNode<>(c, storage);
        }

        // The message can be used to initialize a raft node or not.
        private boolean isInitialMsg(Eraftpb.Message msg) {
            var msgType = msg.getMsgType();
            return msgType == MsgRequestVote || msgType == MsgRequestPreVote || (msgType == MsgHeartbeat && msg.getCommit() == 0);
        }

    }

    // Create a raft leader only with itself in its configuration.
    private static Node createRaftLeader(long id, Map<Long, Sender> mailboxes, Receiver receiver) throws RaftErrorException {
        var node = new Node();
        var c = exampleConfig(id);
        MemStorage memStorage = new MemStorage(Eraftpb.ConfState.newBuilder().addVoters(id).build());
        node.raftGroup = new RawNode<>(c, memStorage);
        node.kv = new HashMap<>();
        node.receiver = receiver;
        node.mailboxes = mailboxes;
        return node;
    }

    // Create a raft follower.
    private static Node createRaftFollower(Map<Long, Sender> mailboxes, Receiver receiver) {
        var node = new Node();
        node.kv = new HashMap<>();
        node.receiver = receiver;
        node.mailboxes = mailboxes;
        return node;
    }

    private static Config exampleConfig(long id) {
        var c = new Config();
        c.setId(id);
        c.setHeartbeatTick(3);
        c.setElectionTick(10);
        return c;
    }

    private static Receiver transport(long id, Map<Long, Sender> mailboxes) {
        var queue = new LinkedBlockingQueue<Eraftpb.Message>();
        mailboxes.put(id, new Sender(queue));
        return new Receiver(queue);
    }

    @SneakyThrows
    private static void propose(RawNode<MemStorage> raftGroup, Proposal proposal) {
        var lastIndex1 = raftGroup.getRaft().getRaftLog().lastIndex() + 1;
        out: {
            var normal = proposal.normal;
            if (normal != null) {
                var data = ByteString.copyFromUtf8(String.format("put %d %s", normal.key, normal.value));
                raftGroup.propose(ByteString.EMPTY, data);
                break out;
            }

            var cc = proposal.confChange;
            if (cc != null) {
                raftGroup.proposeConfChange(ByteString.EMPTY, cc);
            }

            // TODO: implement transfer leader.
        }

        var lastIndex2 = raftGroup.getRaft().getRaftLog().lastIndex() + 1;
        if (lastIndex2 == lastIndex1) {
            // Propose failed, don't forget to respond to the client.
            proposal.done(false);
        } else {
            proposal.proposed = lastIndex1;
        }
    }

    private static class Proposal {
        private Pair normal;
        private Eraftpb.ConfChange confChange;
        private Long transferLeader;
        private long proposed;
        LinkedBlockingQueue<Boolean> ack;

        public Proposal(Eraftpb.ConfChange cc) {
            this.confChange = cc;
            this.ack = new LinkedBlockingQueue<>(1);
        }

        public Proposal(long key, String value) {
            this.normal = new Pair(key, value);
            this.ack = new LinkedBlockingQueue<>(1);
        }

        public void done(boolean result) {
            this.ack.offer(result);
        }

        @SneakyThrows
        public void await() {
            this.ack.take();
        }

    }

    @AllArgsConstructor
    private static class Pair {
        private long key;
        private String value;
    }

}
