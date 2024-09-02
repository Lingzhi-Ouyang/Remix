package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.TestingDef;
import org.disalg.met.api.TestingRemoteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.ArrayList;

public privileged aspect CommitProcessorAspect {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessorAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    private TestingRemoteService testingService;

    private int subnodeId;

    // Intercept starting the CommitProcessor thread

    pointcut runCommitProcessor(): execution(* CommitProcessor.run());

//    before(): runCommitProcessor() {
//        LOG.debug("-------before runCommitProcessor. Thread {}: {}------",Thread.currentThread().getId(), Thread.currentThread().getName());
//        testingService = quorumPeerAspect.createRmiConnection();
//        subnodeId = quorumPeerAspect.registerSubnode(testingService, SubnodeType.COMMIT_PROCESSOR);
//        try {
//            testingService.setReceivingState(subnodeId);
//        } catch (final RemoteException e) {
//            LOG.debug("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
//
//    }

    before(): runCommitProcessor() {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before runCommitProcessor-------Thread: {}, {}------", threadId, threadName);
        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.registerSubnode(
                Thread.currentThread().getId(), Thread.currentThread().getName(), SubnodeType.COMMIT_PROCESSOR);
        try{
            subnodeId = intercepter.getSubnodeId();
            testingService = intercepter.getTestingService();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("before runCommitProcessor-------Thread: {}, subnodeId < 0: {}, " +
                    "indicating the node is STOPPING or OFFLINE. " +
                    "This subnode is not registered at the testing engine." +
                    "------", threadId, subnodeId);
            return;
        }
        try {
            testingService.setReceivingState(subnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    after(): runCommitProcessor() {
        LOG.debug("after runCommitProcessor");
        quorumPeerAspect.deregisterSubnode(testingService, subnodeId, SubnodeType.COMMIT_PROCESSOR);
    }

    /***
     *  intercept adding a request to the queue toProcess
     *  The target method is called in CommitProcessor's run()
     *   --> FollowerProcessCOMMIT
     */

//    // For version 3.4
//    pointcut addToProcess(ArrayList queue, Object object):
//            withincode(* org.apache.zookeeper.server.quorum.CommitProcessor.run())
//                    && call(* ArrayList.add(java.lang.Object))
//                    && if (object instanceof Request)
//                    && target(queue) && args(object);

//    before(ArrayList queue, Object object): addToProcess(queue, object) {
//        final long threadId = Thread.currentThread().getId();
//        final String threadName = Thread.currentThread().getName();
//        LOG.debug("before advice of CommitProcessor.addToProcess()-------Thread: {}, {}------", threadId, threadName);
//        final Request request = (Request) object;
//        LOG.debug("--------------Before addToProcess {}: My toProcess has {} element. commitSubnode: {}",
//                request, queue.size(), subnodeId);
//        if (subnodeId == TestingDef.RetCode.NODE_CRASH) {
//            LOG.debug("COMMIT threadId: {}, subnodeId == -1, indicating the node is STOPPING or OFFLINE", threadId);
//            return;
//        }
//        final int type =  request.type;
////        switch (type) {
////            case TestingDef.OpCode.notification:
////            case TestingDef.OpCode.getData:
////            case TestingDef.OpCode.exists:
////            case TestingDef.OpCode.check:
////            case TestingDef.OpCode.getACL:
////            case TestingDef.OpCode.getChildren:
////            case TestingDef.OpCode.getChildren2:
////            case TestingDef.OpCode.ping:
////            case TestingDef.OpCode.setWatches:
////                LOG.debug("Won't intercept toProcess request: {} ", request);
////                return;
////            default:
////        }
//        try {
//            // before offerMessage: increase sendingSubnodeNum
//            quorumPeerAspect.setSubnodeSending();
//            final String payload = quorumPeerAspect.constructRequest(request);
//            final long zxid = request.zxid;
//            final int lastCommitRequestId =
//                    testingService.offerLocalEvent(subnodeId, SubnodeType.COMMIT_PROCESSOR, zxid, payload, type);
//            LOG.debug("lastCommitRequestId = {}", lastCommitRequestId);
//            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
//            quorumPeerAspect.postSend(subnodeId, lastCommitRequestId);
//            // set RECEIVING state
//            testingService.setReceivingState(subnodeId);
//        } catch (final RemoteException e) {
//            LOG.debug("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
//    }

    // For version 3.6 & 3.7 & 3.8: multi-threads
    pointcut processWrite(Request request):
            call(* org.apache.zookeeper.server.quorum.CommitProcessor.processWrite(Request))
            && args(request);

    before(Request request): processWrite(request) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of CommitProcessor.addToProcess()-------Thread: {}, {}------", threadId, threadName);
        LOG.debug("--------------Before processWrite in CommitProcessor {}: commitSubnode: {}",
                request, subnodeId);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        Integer lastMsgId = null;
        try{
            lastMsgId = intercepter.getLastMsgId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("CommitProcessor threadId: {}, subnodeId == {}, indicating the node is STOPPING or OFFLINE. " +
                            "This subnode is not registered at the testing engine.",
                    threadId, subnodeId);
            return;
        }
        if (lastMsgId != null && lastMsgId.equals(TestingDef.RetCode.BACK_TO_LOOKING)) {
            LOG.debug("CommitProcessor threadId: {}, subnodeId: {}, lastMsgId: {}," +
                    " indicating the node is going to become looking", threadId, subnodeId, lastMsgId);
            return;
        }

        final int type =  request.type;
        try {
            // before offerMessage: increase sendingSubnodeNum
            quorumPeerAspect.setSubnodeSending();
            final String payload = quorumPeerAspect.constructRequest(request);
            final long zxid = request.zxid;
            final int lastCommitRequestId =
                    testingService.offerLocalEvent(subnodeId, SubnodeType.COMMIT_PROCESSOR, zxid, payload, type);
            intercepter.setLastMsgId(lastCommitRequestId);
            LOG.debug("lastCommitRequestId = {}", lastCommitRequestId);
            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(subnodeId, lastCommitRequestId);
            // set RECEIVING state
            testingService.setReceivingState(subnodeId);

            if (lastCommitRequestId == TestingDef.RetCode.BACK_TO_LOOKING) {
                LOG.debug("Sync threadId: {}, event == -200, indicating the node is going to become looking", threadId);
            }

        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }
}
