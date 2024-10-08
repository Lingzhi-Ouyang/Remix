package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerAspect;
import org.disalg.remix.api.MetaDef;
import org.disalg.remix.api.RemoteService;
import org.disalg.remix.api.SubnodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.concurrent.BlockingQueue;

public aspect SyncRequestProcessorAspect {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessorAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    private RemoteService remoteService;

    private int subnodeId;

    public RemoteService getRemoteService() {
        return remoteService;
    }

    // Intercept starting the SyncRequestProcessor thread

    pointcut runSyncProcessor(): execution(* SyncRequestProcessor.run());

    before(): runSyncProcessor() {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before runSyncProcessor-------Thread: {}, {}------", threadId, threadName);
        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.registerSubnode(
                Thread.currentThread().getId(), Thread.currentThread().getName(), SubnodeType.SYNC_PROCESSOR);
        try{
            subnodeId = intercepter.getSubnodeId();
            remoteService = intercepter.getRemoteService();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("before runSyncProcessor-------Thread: {}, subnodeId < 0: {}, " +
                    "indicating the node is STOPPING or OFFLINE. " +
                    "This subnode is not registered at the replay engine." +
                    "------", threadId, subnodeId);
            return;
        }
        quorumPeerAspect.setSyncSubnodeId(subnodeId);
    }

    after(): runSyncProcessor() {
        LOG.debug("after runSyncProcessor");
        quorumPeerAspect.setSyncSubnodeId(-1);
        quorumPeerAspect.deregisterSubnode(remoteService, subnodeId, SubnodeType.SYNC_PROCESSOR);
    }


    // Intercept message processed within SyncRequestProcessor
    // candidate 1: processRequest() called by its previous processor
    // Use candidate 2: LinkedBlockingQueue.take() / poll()

    /***
     * Intercept polling the receiving queue of the queuedRequests within SyncRequestProcessor
     *  --> FollowerProcessPROPOSAL
     */

    // For version 3.6+
    pointcut takeOrPollFromQueue(BlockingQueue queue):
            within(SyncRequestProcessor)
                    && (call(* BlockingQueue.take())
                    || call(* BlockingQueue.poll(..)))
                    && target(queue);

    before(final BlockingQueue queue): takeOrPollFromQueue(queue) {
        if (queue.isEmpty()) {
            // Going to block here. Better notify the scheduler
            try {
                remoteService.setReceivingState(subnodeId);
            } catch (final RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
    }

    after(final BlockingQueue queue) returning (final Object request): takeOrPollFromQueue(queue) {
        // TODO: Aspect of aspect
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("after advice of sync-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        Integer lastMsgId = null;
        try{
            lastMsgId = intercepter.getLastMsgId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("Sync threadId: {}, subnodeId == {}, indicating the node is STOPPING or OFFLINE. " +
                            "This subnode is not registered at the replay engine.",
                    threadId, subnodeId);
            return;
        }

        if (request == null){
            LOG.debug("------Using poll() just now and found no request! Flush now and using take()...");
            return;
        }
        if (request instanceof Request) {
            LOG.debug("Take the request for logging : {}", request);
            final String payload = quorumPeerAspect.constructRequest((Request) request);
            final int type =  ((Request) request).type;
            try {
                // before offerMessage: increase sendingSubnodeNum
                quorumPeerAspect.setSubnodeSending();
                final long zxid = ((Request) request).zxid;
                final int lastSyncRequestId =
                        remoteService.offerLocalEvent(subnodeId, SubnodeType.SYNC_PROCESSOR, zxid, payload, type);
                LOG.debug("lastSyncRequestId = {}", lastSyncRequestId);
                intercepter.setLastMsgId(lastSyncRequestId);
                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
                quorumPeerAspect.postSend(intercepter, subnodeId, lastSyncRequestId);

                if (lastSyncRequestId == MetaDef.RetCode.BACK_TO_LOOKING) {
                    LOG.debug("Sync threadId: {}, event == -200, indicating the node is going to become looking", threadId);
                }

            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }

}
