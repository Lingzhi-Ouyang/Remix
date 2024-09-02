package org.apache.zookeeper.server.quorum;

import org.apache.jute.Record;
import org.disalg.met.api.MessageType;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.TestingDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/***
 * This intercepts the message sending process of the learnerHandler threads on the leader side
 */
public aspect LearnerHandlerAspect {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandlerAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    private static Map<Long, Long> learnerHandlerSenderThreadMap = new HashMap<>(); // key: learnerHandlerThreadId, value: learnerHandlerSenderThreadId
    private static Map<Long, Long> learnerHandlerThreadMap = new HashMap<>(); // key: learnerHandlerSenderThreadId, value: learnerHandlerThreadId

    // Intercept starting the thread
    // This thread should only be run by the leader

    pointcut runLearnerHandler(): execution(* org.apache.zookeeper.server.quorum.LearnerHandler.run());

    before(): runLearnerHandler() {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before runLearnerHandler-------Thread: {}, {}------", threadId, threadName);
        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.registerSubnode(
                Thread.currentThread().getId(), Thread.currentThread().getName(), SubnodeType.LEARNER_HANDLER);
        // Set RECEIVING state since there is nowhere else to set
        int subnodeId = -1;
        try{
            subnodeId = intercepter.getSubnodeId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("before runLearnerHandler-------Thread: {}, subnodeId < 0: {}, " +
                    "indicating the node is STOPPING or OFFLINE. " +
                    "This subnode is not registered at the testing engine.\" +" +
                    "------", threadId, subnodeId);
            return;
        }
        try {
            intercepter.getTestingService().setReceivingState(subnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    after(): runLearnerHandler() {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("after runLearnerHandler-------Thread: {}, {}------", threadId, threadName);
        Long learnerHandlerThreadId = Thread.currentThread().getId();
        quorumPeerAspect.deregisterSubnode(learnerHandlerThreadId);
//        assert learnerHandlerSenderMap.containsKey(learnerHandlerThreadId);
        Long learnerHandlerSenderThreadId = learnerHandlerSenderThreadMap.get(learnerHandlerThreadId); // may be null in discovery phase
        if (learnerHandlerThreadId != null) {
            quorumPeerAspect.deregisterSubnode(learnerHandlerSenderThreadId);
            LOG.debug("de-registered: learnerHandlerThreadId: {} - learnerHandlerSenderThreadId: {}",
                    learnerHandlerThreadId, learnerHandlerSenderThreadId);
            learnerHandlerSenderThreadMap.remove(learnerHandlerThreadId);
            learnerHandlerThreadMap.remove(learnerHandlerSenderThreadId);
        }
    }


    // intercept the sender thread created by a learner handler

    // For version 3.4.X
//    pointcut runLearnerHandlerSender(java.lang.Thread childThread):
//            withincode(* org.apache.zookeeper.server.quorum.LearnerHandler.run())
//                && call(* java.lang.Thread.start())
//                && target(childThread);

    // For version 3.5 & 3.6 & 3.7 & 3.8
    pointcut runLearnerHandlerSender(java.lang.Thread childThread):
            withincode(* org.apache.zookeeper.server.quorum.LearnerHandler.startSendingPackets())
                    && call(* java.lang.Thread.start())
                    && target(childThread);

    before(java.lang.Thread childThread): runLearnerHandlerSender(childThread) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        final long childThreadId = childThread.getId();
        final String childThreadName = childThread.getName();
        LOG.debug("before runSender-------parent thread {}: {}------", threadId, threadName);
        LOG.debug("before runSender-------child Thread {}: {}------", childThreadId, childThreadName);
        QuorumPeerAspect.SubnodeIntercepter intercepter =
                quorumPeerAspect.registerSubnode(childThreadId, childThreadName, SubnodeType.LEARNER_HANDLER_SENDER);
        learnerHandlerSenderThreadMap.put(threadId, childThreadId);
        learnerHandlerThreadMap.put(childThreadId, threadId);
        int subnodeId = -1;
        try{
            subnodeId = intercepter.getSubnodeId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("before runLearnerHandlerSender-------Thread: {}, subnodeId < 0: {}, " +
                    "indicating the node is STOPPING or OFFLINE. " +
                    "This subnode is not registered at the testing engine.\" +" +
                    "------", threadId, subnodeId);
            return;
        }
    }



    /***
     * For LearnerHandlerSender
     * Set RECEIVING state when the queue is empty
     */
    pointcut takeOrPollFromQueue(LinkedBlockingQueue queue):
            within(org.apache.zookeeper.server.quorum.LearnerHandler)
                    && (call(* LinkedBlockingQueue.poll()) || call(* LinkedBlockingQueue.take()))
                    && target(queue);

    before(final LinkedBlockingQueue queue): takeOrPollFromQueue(queue) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of learner handler send-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId = -1;
        Integer lastMsgId = null;
        try{
            subnodeId = intercepter.getSubnodeId();
            lastMsgId = intercepter.getLastMsgId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("LearnerHandler threadId: {}, subnodeId == {}, indicating the node is STOPPING or OFFLINE. " +
                            "This subnode is not registered at the testing engine.",
                    threadId, subnodeId);
            return;
        }
        if (lastMsgId != null && lastMsgId.equals(TestingDef.RetCode.BACK_TO_LOOKING)) {
            LOG.debug("LearnerHandler threadId: {}, subnodeId: {}, lastMsgId: {}," +
                    " indicating the node is going to become looking", threadId, subnodeId, lastMsgId);
            return;
        }

        LOG.debug("--------------My queuedPackets has {} element.",
                queue.size());

        if (queue.isEmpty()) {
            // Going to block here. Better notify the scheduler
            LOG.debug("--------------Checked empty! My queuedPackets has {} element. Set subnode {} to RECEIVING state." +
                    " Will be blocked until some packet enqueues", queue.size(), subnodeId);
            try {
                intercepter.getTestingService().setReceivingState(subnodeId);
            } catch (final RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
    }


    /***
     * For LearnerHandlerSender sending messages to followers
     * - During SYNC phase
     *  --> LeaderSyncFollower:
     *      --> in zk-3.4: send DIFF / TRUNC / SNAP done by LearnerHandler
     *      --> in zk-3.8: send DIFF / TRUNC / SNAP done by LearnerHandlerSender
     *      THEN send PROPOSAL & COMMIT in SYNC phase
     *      THEN send NEWLEADER
     *  --> LeaderProcessACKLD: send UPTODATE
     * - During BROADCAST phase
     *  --> LeaderProcessRequest: send PROPOSAL to quorum followers
     *  --> LeaderProcessACK : send COMMIT after receiving quorum's logRequest (PROPOSAL) ACKs
     */
    pointcut writeRecord(Record r, String s):
            within(org.apache.zookeeper.server.quorum.LearnerHandler) && !withincode(void java.lang.Runnable.run()) &&
            call(* org.apache.jute.BinaryOutputArchive.writeRecord(Record, String)) && args(r, s);

    void around(Record r, String s): writeRecord(r, s) {
        LOG.debug("------around-before writeRecord");
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of learner handler sender-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId = -1;
        Integer lastMsgId = null;
        try{
            subnodeId = intercepter.getSubnodeId();
            lastMsgId = intercepter.getLastMsgId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("LearnerHandlerSender threadId: {}, subnodeId == {}, indicating the node is STOPPING or OFFLINE. " +
                            "This subnode is not registered at the testing engine.",
                    threadId, subnodeId);
            return;
        }
        if (lastMsgId != null && lastMsgId.equals(TestingDef.RetCode.BACK_TO_LOOKING)) {
            LOG.debug("LearnerHandlerSender threadId: {}, subnodeId: {}, lastMsgId: {}," +
                    " indicating the node is going to become looking", threadId, subnodeId, lastMsgId);
            proceed(r, s);
            return;
        }
        QuorumPacket packet = (QuorumPacket) r;
        final String payload = quorumPeerAspect.packetToString(packet);
        final int type =  packet.getType();
        LOG.debug("---------Taking the packet ({}) from queued packets. Subnode: {}",
                            payload, subnodeId);


        try {
            // before offerMessage: increase sendingSubnodeNum
            if (type != Leader.PING) {
                quorumPeerAspect.setSubnodeSending(intercepter);
            }

            final String receivingAddr = threadName.split("-")[1];
            final long zxid = packet.getZxid();
            final int lastPacketId = intercepter.getTestingService()
                    .offerLeaderToFollowerMessage(subnodeId, receivingAddr, zxid, payload, type);
            intercepter.setLastMsgId(lastPacketId);
            LOG.debug("lastPacketId = {}", lastPacketId);

            // to check if the node is crashed
            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            if (type != Leader.PING) {
                quorumPeerAspect.postSend(intercepter, subnodeId, lastPacketId);
            }


            // Trick: set RECEIVING state here
            intercepter.getTestingService().setReceivingState(subnodeId);

            // to check if the partition happens
            if (lastPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message.");
                throw new IOException();
            }
            if (lastPacketId == TestingDef.RetCode.BACK_TO_LOOKING) {
                LOG.debug("LearnerHandlerSender threadId: {}, event == -200, indicating the node is going to become looking", threadId);
            }

            proceed(r, s);
        } catch (RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.debug("Encountered an IO exception", e);
            throw new RuntimeException(e);
        }

    }



    /***
     * For LearnerHandler reading record during DISCOVERY & SYNC
     * Related code: LearnerHandler.java
     */
    pointcut learnerHandlerReadRecord(Record r, String s):
            withincode(* org.apache.zookeeper.server.quorum.LearnerHandler.run()) &&
                    call(* org.apache.jute.BinaryInputArchive.readRecord(Record, String)) && args(r, s);

    before(Record r, String s): learnerHandlerReadRecord(r, s) {
        LOG.debug("------before learnerHandlerReadRecord");
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of learnerHandlerReadRecord-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId = -1;
        Integer lastMsgId = null;
        try{
            subnodeId = intercepter.getSubnodeId();
            lastMsgId = intercepter.getLastMsgId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("LearnerHandler threadId: {}, subnodeId == {}, indicating the node is STOPPING or OFFLINE. " +
                            "This subnode is not registered at the testing engine.",
                    threadId, subnodeId);
            return;
        }
        if (lastMsgId != null && lastMsgId.equals(TestingDef.RetCode.BACK_TO_LOOKING)) {
            LOG.debug("LearnerHandler threadId: {}, subnodeId: {}, lastMsgId: {}," +
                    " indicating the node is going to become looking", threadId, subnodeId, lastMsgId);
            return;
        }


        try {
            // before offerMessage: increase sendingSubnodeNum
            quorumPeerAspect.setSubnodeSending(intercepter);

            final String receivingAddr = threadName.split("-")[1];
            final int lastPacketId = intercepter.getTestingService().offerLeaderToFollowerMessage(
                    subnodeId, receivingAddr, -1L, null, TestingDef.MessageType.learnerHandlerReadRecord);
            intercepter.setLastMsgId(lastPacketId);
            LOG.debug("learnerHandlerReadRecord lastPacketId = {}", lastPacketId);

            quorumPeerAspect.postSend(intercepter, subnodeId, lastPacketId);

            // Trick: set RECEIVING state here
            intercepter.getTestingService().setReceivingState(subnodeId);

            // to check if the partition happens
            if (lastPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message.");
                throw new SocketTimeoutException();
//                return;
            }
            if (lastPacketId == TestingDef.RetCode.BACK_TO_LOOKING) {
                LOG.debug("LearnerHandler threadId: {}, event == -200, indicating the node is going to become looking", threadId);
            }

        } catch (RemoteException | SocketTimeoutException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    /***
     * For LearnerHandler sending followers' message during SYNC phase immediately without adding to the queue
     * package type:
     * (for ZAB1.0) LEADERINFO (17)
     * (for ZAB < 1.0) NEWLEADER (10)
     * (for ZAB1.0) DIFF (13) / TRUNC (14) / SNAP (15)
     * Note:
     *  --> in zk-3.4: send DIFF / TRUNC / SNAP done by LearnerHandler
     *  --> in zk-3.4: only for intercepting LeaderSyncFollower in ZAB1.0 :
     *          send DIFF / TRUNC / SNAP (intercepted in LearnerHandler)
     *              & NEWLEADER (intercepted in LearnerHandlerSender)
     *  --> in zk-3.8: send SNAP done by LearnerHandler
     *                 send DIFF / TRUNC  done by LearnerHandlerSender
     */
    pointcut learnerHandlerWriteRecord(Record r, String s):
            withincode(* org.apache.zookeeper.server.quorum.LearnerHandler.run()) &&
                    call(* org.apache.jute.BinaryOutputArchive.writeRecord(Record, String)) && args(r, s);

    void around(Record r, String s): learnerHandlerWriteRecord(r, s) {
        LOG.debug("------around-before learnerHandlerWriteRecord");
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of learner handler-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId = -1;
        Integer lastMsgId = null;
        try{
            subnodeId = intercepter.getSubnodeId();
            lastMsgId = intercepter.getLastMsgId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("LearnerHandler threadId: {}, subnodeId == {}, indicating the node is STOPPING or OFFLINE. " +
                            "This subnode is not registered at the testing engine.",
                    threadId, subnodeId);
            return;
        }
        if (lastMsgId != null && lastMsgId.equals(TestingDef.RetCode.BACK_TO_LOOKING)) {
            LOG.debug("LearnerHandler threadId: {}, subnodeId: {}, lastMsgId: {}," +
                    " indicating the node is going to become looking", threadId, subnodeId, lastMsgId);
            proceed(r, s);
            return;
        }

        // Intercept QuorumPacket
        QuorumPacket packet = (QuorumPacket) r;
        final String payload = quorumPeerAspect.packetToString(packet);

        final int type =  packet.getType();
        LOG.debug("--------------I am a LearnerHandler. QuorumPacket {}. Set subnode {} to RECEIVING state. Type: {}",
                payload, subnodeId, type);

        try {

            // before offerMessage: increase sendingSubnodeNum
            quorumPeerAspect.setSubnodeSending(intercepter);

            final String receivingAddr = threadName.split("-")[1];
            final long zxid = packet.getZxid();
            final int lastPacketId = intercepter.getTestingService()
                    .offerLeaderToFollowerMessage(subnodeId, receivingAddr, zxid, payload, type);
            intercepter.setLastMsgId(lastPacketId);
            LOG.debug("learnerHandlerWriteRecord lastPacketId = {}", lastPacketId);

            quorumPeerAspect.postSend(intercepter, subnodeId, lastPacketId);

            // Trick: set RECEIVING state here
            intercepter.getTestingService().setReceivingState(subnodeId);

            // to check if the partition happens
            if (lastPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message.");
                throw new SocketTimeoutException();
            }
            if (lastPacketId == TestingDef.RetCode.BACK_TO_LOOKING) {
                LOG.debug("LearnerHandler threadId: {}, event == -200, indicating the node is going to become looking", threadId);
            }

            proceed(r, s);
        } catch (RemoteException | SocketTimeoutException e ) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * For LearnerHandler starting sync with its corresponding follower at the beginning of SYNC.
     * Note: un-exist in zk-3.4
     */
    pointcut learnerHandlerSyncFollower(long zxid, LearnerMaster lm):
            withincode(* org.apache.zookeeper.server.quorum.LearnerHandler.run())
                    && call(* org.apache.zookeeper.server.quorum.LearnerHandler.syncFollower(long, LearnerMaster))
                    && args(zxid, lm);

    before(long zxid, LearnerMaster lm): learnerHandlerSyncFollower(zxid, lm) {
        LOG.debug("------before learnerHandlerSyncFollower");
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of learner handler-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId = -1;
        Integer lastMsgId = null;
        try{
            subnodeId = intercepter.getSubnodeId();
            lastMsgId = intercepter.getLastMsgId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("LearnerHandler threadId: {}, subnodeId == {}, indicating the node is STOPPING or OFFLINE. " +
                            "This subnode is not registered at the testing engine.",
                    threadId, subnodeId);
            return;
        }
        if (lastMsgId != null && lastMsgId.equals(TestingDef.RetCode.BACK_TO_LOOKING)) {
            LOG.debug("LearnerHandler threadId: {}, subnodeId: {}, lastMsgId: {}," +
                    " indicating the node is going to become looking", threadId, subnodeId, lastMsgId);
            return;
        }

        try {
            // before offerMessage: increase sendingSubnodeNum
            quorumPeerAspect.setSubnodeSending(intercepter);

            final String receivingAddr = threadName.split("-")[1];

            // Trick: actually this is a local event. We make it a message event to record the syncing follower
            final int eventId = intercepter.getTestingService()
                    .offerLocalEvent(subnodeId, SubnodeType.LEARNER_HANDLER, zxid, receivingAddr, MessageType.ACKEPOCH);
            intercepter.setLastMsgId(eventId);
            LOG.debug("learnerHandler about to sync. eventId = {}", eventId);

            quorumPeerAspect.postSend(intercepter, subnodeId, eventId);

            // Trick: set RECEIVING state here
            intercepter.getTestingService().setReceivingState(subnodeId);

            if (eventId == TestingDef.RetCode.BACK_TO_LOOKING) {
                LOG.debug("LearnerHandler threadId: {}, event == -200, indicating the node is going to become looking", threadId);
            }

        } catch (RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

}
