package org.apache.zookeeper.server.quorum;

import org.disalg.met.api.TestingDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public aspect WorkerReceiverAspect {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerReceiverAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    private Integer lastSentMessageId = null;

    private int workerReceiverSubnodeId;

    // Keep track of the message we're replying to
    private QuorumCnxManager.Message response;

    public QuorumCnxManager.Message getResponse() {
        return response;
    }

    private final AtomicInteger msgsInRecvQueue = new AtomicInteger(0);

    public AtomicInteger getMsgsInRecvQueue() {
        return msgsInRecvQueue;
    }

    // Intercept starting the WorkerReceiver thread

    pointcut runWorkerReceiver(): execution(* FastLeaderElection.Messenger.WorkerReceiver.run());

    before(): runWorkerReceiver() {
        LOG.debug("-------runWorkerReceiver Thread: {}------", Thread.currentThread().getName());
        workerReceiverSubnodeId = quorumPeerAspect.registerWorkerReceiverSubnode();
    }

    after(): runWorkerReceiver() {
        quorumPeerAspect.deregisterWorkerReceiverSubnode(workerReceiverSubnodeId);
    }

    // Intercept message offering within WorkerReceiver

    pointcut offerWithinWorkerReceiver(Object object):
            within(FastLeaderElection.Messenger.WorkerReceiver)
            && call(* java.util.concurrent.LinkedBlockingQueue.offer(Object))
            && if (object instanceof FastLeaderElection.ToSend)
            && args(object);

    boolean around(final Object object): offerWithinWorkerReceiver(object) {
        final FastLeaderElection.ToSend toSend = (FastLeaderElection.ToSend) object;

        final Set<Integer> predecessorIds = new HashSet<>();
        predecessorIds.add(response.getMessageId());
        if (null != lastSentMessageId) {
            predecessorIds.add(lastSentMessageId);
        }

        try {
            LOG.debug("WorkerReceiver subnode {} is offering a message with predecessors {}", workerReceiverSubnodeId, predecessorIds.toString());
            // before offerMessage: increase sendingSubnodeNum
            quorumPeerAspect.setSubnodeSending();
            final String payload = quorumPeerAspect.constructPayload(toSend);
            lastSentMessageId = quorumPeerAspect.getTestingService().offerElectionMessage(workerReceiverSubnodeId,
                    (int) toSend.sid, toSend.electionEpoch, (int) toSend.leader, predecessorIds, payload);
            LOG.debug("lastSentMessageId = {}", lastSentMessageId);
            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(workerReceiverSubnodeId, lastSentMessageId);

            if (lastSentMessageId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message.");

                quorumPeerAspect.getTestingService().setReceivingState(workerReceiverSubnodeId);
                // confirm the return value
                return false;
            }
            return proceed(object);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Intercept forwarding a notification to FLE.recvqueue

    pointcut forwardNotification(Object object):
            within(FastLeaderElection.Messenger.WorkerReceiver)
            && call(* java.util.concurrent.LinkedBlockingQueue.offer(Object))
            && if (object instanceof FastLeaderElection.Notification)
            && args(object);

    before(final Object object): forwardNotification(object) {
        try {
            quorumPeerAspect.getTestingService().setProcessingState(quorumPeerAspect.getQuorumPeerSubnodeId());
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Intercept polling the receive queue of the QuorumCnxManager

    pointcut pollRecvQueue():
            within(FastLeaderElection.Messenger.WorkerReceiver)
            && call(* QuorumCnxManager.pollRecvQueue(..));

    before(): pollRecvQueue() {
        if (msgsInRecvQueue.get() == 0) {
            // Going to block here. Better notify the scheduler
            LOG.debug("My QCM.recvQueue is empty, go to RECEIVING state");
            try {
                quorumPeerAspect.getTestingService().setReceivingState(workerReceiverSubnodeId);
            } catch (final RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
    }

    after() returning (final QuorumCnxManager.Message response): pollRecvQueue() {
        if (null != response) {
            LOG.debug("Received a message with id = {}", response.getMessageId());
            msgsInRecvQueue.decrementAndGet();
            this.response = response;
        }
    }
}
