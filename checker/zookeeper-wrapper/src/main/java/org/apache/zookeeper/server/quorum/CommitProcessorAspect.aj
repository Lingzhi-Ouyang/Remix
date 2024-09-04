package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.disalg.remix.api.MetaDef;
import org.disalg.remix.api.RemoteService;
import org.disalg.remix.api.SubnodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

public privileged aspect CommitProcessorAspect {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessorAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    private RemoteService remoteService;

    private int subnodeId;

    // Intercept starting the CommitProcessor thread

    pointcut runCommitProcessor(): execution(* CommitProcessor.run());

    before(): runCommitProcessor() {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before runCommitProcessor-------Thread: {}, {}------", threadId, threadName);
        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.registerSubnode(
                Thread.currentThread().getId(), Thread.currentThread().getName(), SubnodeType.COMMIT_PROCESSOR);
        try{
            subnodeId = intercepter.getSubnodeId();
            remoteService = intercepter.getRemoteService();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        if (subnodeId < 0) {
            LOG.debug("before runCommitProcessor-------Thread: {}, subnodeId < 0: {}, " +
                    "indicating the node is STOPPING or OFFLINE. " +
                    "This subnode is not registered at the replay engine." +
                    "------", threadId, subnodeId);
            return;
        }
        try {
            remoteService.setReceivingState(subnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    after(): runCommitProcessor() {
        LOG.debug("after runCommitProcessor");
        quorumPeerAspect.deregisterSubnode(remoteService, subnodeId, SubnodeType.COMMIT_PROCESSOR);
    }

    /***
     *  intercept adding a request to the queue toProcess
     *  The target method is called in CommitProcessor's run()
     *   --> FollowerProcessCOMMIT
     */

    // For version 3.6+: multi-threads
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
                            "This subnode is not registered at the replay engine.",
                    threadId, subnodeId);
            return;
        }
        if (lastMsgId != null && lastMsgId.equals(MetaDef.RetCode.BACK_TO_LOOKING)) {
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
                    remoteService.offerLocalEvent(subnodeId, SubnodeType.COMMIT_PROCESSOR, zxid, payload, type);
            intercepter.setLastMsgId(lastCommitRequestId);
            LOG.debug("lastCommitRequestId = {}", lastCommitRequestId);
            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(subnodeId, lastCommitRequestId);
            // set RECEIVING state
            remoteService.setReceivingState(subnodeId);

            if (lastCommitRequestId == MetaDef.RetCode.BACK_TO_LOOKING) {
                LOG.debug("Sync threadId: {}, event == -200, indicating the node is going to become looking", threadId);
            }

        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }
}
