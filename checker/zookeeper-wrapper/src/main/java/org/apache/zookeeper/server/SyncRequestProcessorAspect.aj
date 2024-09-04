package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerAspect;
import org.disalg.remix.api.SubnodeType;
import org.disalg.remix.api.TestingDef;
import org.disalg.remix.api.TestingRemoteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.concurrent.BlockingQueue;

public aspect SyncRequestProcessorAspect {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessorAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    private TestingRemoteService testingService;

    private int subnodeId;

    public TestingRemoteService getTestingService() {
        return testingService;
    }

    // Intercept starting the SyncRequestProcessor thread

    pointcut runSyncProcessor(): execution(* SyncRequestProcessor.run());

//    before(): runSyncProcessor() {
//        testingService = quorumPeerAspect.createRmiConnection();
//        LOG.debug("-------Thread: {}------", Thread.currentThread().getName());
//        LOG.debug("before runSyncProcessor");
//        subnodeId = quorumPeerAspect.registerSubnode(testingService, SubnodeType.SYNC_PROCESSOR);
//        quorumPeerAspect.setSyncSubnodeId(subnodeId);
//    }

    before(): runSyncProcessor() {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before runSyncProcessor-------Thread: {}, {}------", threadId, threadName);
        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.registerSubnode(
                Thread.currentThread().getId(), Thread.currentThread().getName(), SubnodeType.SYNC_PROCESSOR);
        try{
            subnodeId = intercepter.getSubnodeId();
            testingService = intercepter.getTestingService();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("before runSyncProcessor-------Thread: {}, subnodeId < 0: {}, " +
                    "indicating the node is STOPPING or OFFLINE. " +
                    "This subnode is not registered at the testing engine." +
                    "------", threadId, subnodeId);
            return;
        }
        quorumPeerAspect.setSyncSubnodeId(subnodeId);
    }

    after(): runSyncProcessor() {
        LOG.debug("after runSyncProcessor");
        quorumPeerAspect.setSyncSubnodeId(-1);
        quorumPeerAspect.deregisterSubnode(testingService, subnodeId, SubnodeType.SYNC_PROCESSOR);
    }


    // Intercept message processed within SyncRequestProcessor
    // candidate 1: processRequest() called by its previous processor
    // Use candidate 2: LinkedBlockingQueue.take() / poll()

    /***
     * Intercept polling the receiving queue of the queuedRequests within SyncRequestProcessor
     *  --> FollowerProcessPROPOSAL
     */

//    // For version 3.4 & 3.5: LinkedBlockingQueue
//    pointcut takeOrPollFromQueue(LinkedBlockingQueue queue):
//            within(SyncRequestProcessor)
//                    && (call(* LinkedBlockingQueue.take())
//                    || call(* LinkedBlockingQueue.poll()))
//                    && target(queue);

    // For version 3.6 & 3.7 & 3.8: LinkedBlockingQueue
    pointcut takeOrPollFromQueue(BlockingQueue queue):
            within(SyncRequestProcessor)
                    && (call(* BlockingQueue.take())
                    || call(* BlockingQueue.poll(..)))
                    && target(queue);

    before(final BlockingQueue queue): takeOrPollFromQueue(queue) {
        if (queue.isEmpty()) {
            // Going to block here. Better notify the scheduler
            try {
                testingService.setReceivingState(subnodeId);
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
                            "This subnode is not registered at the testing engine.",
                    threadId, subnodeId);
            return;
        }
//        if (lastMsgId != null && lastMsgId.equals(TestingDef.RetCode.BACK_TO_LOOKING)) {
//            LOG.debug("Sync threadId: {}, subnodeId: {}, lastMsgId: {}," +
//                    " indicating the node is going to become looking", threadId, subnodeId, lastMsgId);
//            if (request instanceof Request ) {
//                LOG.debug("It's a request {}", request);
//                Request si = (Request) request;
//                if (request == Request.requestOfDeath) {
//                    LOG.debug("It's going to shutdown!!!!!!! {}", request);
//                } else {
//                    return;
//                }
//            }
//        }

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
                        testingService.offerLocalEvent(subnodeId, SubnodeType.SYNC_PROCESSOR, zxid, payload, type);
                LOG.debug("lastSyncRequestId = {}", lastSyncRequestId);
                intercepter.setLastMsgId(lastSyncRequestId);
                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
                quorumPeerAspect.postSend(intercepter, subnodeId, lastSyncRequestId);

                if (lastSyncRequestId == TestingDef.RetCode.BACK_TO_LOOKING) {
                    LOG.debug("Sync threadId: {}, event == -200, indicating the node is going to become looking", threadId);
                }

            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }

}
