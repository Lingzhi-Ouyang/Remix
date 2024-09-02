package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.util.ZxidUtils;
import org.disalg.met.api.TestingDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public aspect WorkerSenderAspect {

//    private static final Logger LOG = LoggerFactory.getLogger(WorkerSenderAspect.class);
//
//    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();
//
//    private Integer lastSentMessageId = null;
//
//    private Integer lastSentSid = null;
//
//    private int workerSenderSubnodeId;
//
//    pointcut runWorkerSender(): execution(* FastLeaderElection.Messenger.WorkerSender.run());
//
//    before(): runWorkerSender() {
//        LOG.debug("-------runWorkerSender Thread: {}------", Thread.currentThread().getName());
//        workerSenderSubnodeId = quorumPeerAspect.registerWorkerSenderSubnode();
//    }
//
//    after(): runWorkerSender() {
//        quorumPeerAspect.deregisterWorkerSenderSubnode(workerSenderSubnodeId);
//    }
//
//    pointcut process():
//            within(FastLeaderElection.Messenger.WorkerSender)
//                    && call(void process(..));
//
//    before(): process() {
//        LOG.debug("process here");
//    }
//
//
//    pointcut toSend(Long sid, ByteBuffer buffer):
//            execution(* QuorumCnxManager.toSend(Long, ByteBuffer))
//            && args(sid, buffer);
//
//    before(Long sid, ByteBuffer buffer): toSend(sid, buffer) {
//        lastSentSid = Math.toIntExact(sid);
//    }
//
//    pointcut offerWithinWorkerSender(ArrayBlockingQueue queue, ByteBuffer buffer):
//            withincode(* QuorumCnxManager.toSend(..))
//                    && call(* QuorumCnxManager.addToSendQueue(ArrayBlockingQueue, ByteBuffer))
//                    && args(queue, buffer);
//
//    void around(ArrayBlockingQueue queue, ByteBuffer buffer): offerWithinWorkerSender(queue, buffer) {
//        FastLeaderElection.Notification n = new FastLeaderElection.Notification();
//        // State of peer that sent this message
//        QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
//        LOG.debug("before reading the buffer");
//        switch (buffer.getInt()) {
//            case 0:
//                ackstate = QuorumPeer.ServerState.LOOKING;
//                break;
//            case 1:
//                ackstate = QuorumPeer.ServerState.FOLLOWING;
//                break;
//            case 2:
//                ackstate = QuorumPeer.ServerState.LEADING;
//                break;
//            case 3:
//                ackstate = QuorumPeer.ServerState.OBSERVING;
//                break;
//        }
//        n.leader = buffer.getLong();
//        n.zxid = buffer.getLong();
//        n.electionEpoch = buffer.getLong();
//        n.state = ackstate;
//        n.sid = lastSentSid;
//        n.peerEpoch = buffer.getLong();
//        n.version = (buffer.remaining() >= 4) ?
//                buffer.getInt() : 0x0;
//
//        LOG.info("notification: {}", n);
//
//        final Set<Integer> predecessorIds = new HashSet<>();
////        predecessorIds.add(response.getMessageId());
//        if (null != lastSentMessageId) {
//            predecessorIds.add(lastSentMessageId);
//        }
//
//        try {
//            LOG.debug("WorkerSender subnode {} is offering a message with predecessors {}", workerSenderSubnodeId, predecessorIds.toString());
//            // before offerMessage: increase sendingSubnodeNum
//            quorumPeerAspect.setSubnodeSending();
////            final String payload = quorumPeerAspect.constructPayload(toSend);
//            lastSentMessageId = quorumPeerAspect.getTestingService().offerElectionMessage(workerSenderSubnodeId, lastSentSid, predecessorIds, n.toString());
//            LOG.debug("lastSentMessageId = {}", lastSentMessageId);
//            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
//            quorumPeerAspect.postSend(workerSenderSubnodeId, lastSentMessageId);
//
//            quorumPeerAspect.getTestingService().setReceivingState(workerSenderSubnodeId);
//
//            if (lastSentMessageId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
//                // just drop the message
//                LOG.debug("partition occurs! just drop the message.");
//            }
//
//            proceed(queue, buffer);
//        } catch (final RemoteException e) {
//            LOG.debug("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
//    }
}
