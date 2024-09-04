package org.apache.zookeeper.server.quorum;

import org.disalg.remix.api.SubnodeType;
import org.disalg.remix.api.TestingDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

public privileged aspect LeaderAspect {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    pointcut ping():
            withincode(void org.apache.zookeeper.server.quorum.Leader.lead()) &&
                    call(void org.apache.zookeeper.server.quorum.LearnerHandler.ping());

    void around(): ping() {
        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();
        LOG.debug("---------Before ping. Intercept leader judging isRunning. Subnode: {}", quorumPeerSubnodeId);
        try {
            quorumPeerAspect.setSubnodeSending();
            final int judgingRunningPacketId = quorumPeerAspect.getRemoteService()
                    .offerLocalEvent(quorumPeerSubnodeId,
                            SubnodeType.QUORUM_PEER, -1L, null, TestingDef.MessageType.leaderJudgingIsRunning);
            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(quorumPeerSubnodeId, judgingRunningPacketId);

            // Trick: set RECEIVING state here
            quorumPeerAspect.getRemoteService().setReceivingState(quorumPeerSubnodeId);

            // to check if the partition happens
            if (judgingRunningPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs!");
                throw new RuntimeException("judgingRunningPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION");
            }
            return;
        } catch (RemoteException e ) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

}
