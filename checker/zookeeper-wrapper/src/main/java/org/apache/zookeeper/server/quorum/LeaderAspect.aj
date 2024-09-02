package org.apache.zookeeper.server.quorum;

import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.TestingDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

public privileged aspect LeaderAspect {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();


    /***
     * For version 3.4
     * ping(): will not be intercepted
     * isRunning(): intercept and modify its value
     */
//    pointcut ping():
//            withincode(void org.apache.zookeeper.server.quorum.Leader.lead()) &&
//                    call(void org.apache.zookeeper.server.quorum.LearnerHandler.ping());
//
//    void around(): ping() {
//        LOG.debug("before ping");
//        LOG.debug("after ping");
//    }
//
//    pointcut isRunning():
//            withincode(void org.apache.zookeeper.server.quorum.Leader.lead()) &&
//                    call(boolean org.apache.zookeeper.server.quorum.Leader.isRunning());
//
//    boolean around(): isRunning() {
//        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();
//        LOG.debug("---------intercept leader judging isRunning. Subnode: {}", quorumPeerSubnodeId);
//        try {
//            quorumPeerAspect.setSubnodeSending();
//            final int judgingRunningPacketId = quorumPeerAspect.getTestingService()
//                    .offerLocalEvent(quorumPeerSubnodeId,
//                            SubnodeType.QUORUM_PEER, -1L, null, TestingDef.MessageType.leaderJudgingIsRunning);
//            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
//            quorumPeerAspect.postSend(quorumPeerSubnodeId, judgingRunningPacketId);
//
//            // Trick: set RECEIVING state here
//            quorumPeerAspect.getTestingService().setReceivingState(quorumPeerSubnodeId);
//
//            // to check if the partition happens
//            if (judgingRunningPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
//                // just drop the message
//                LOG.debug("partition occurs!");
//                return false;
//            }
//            return false;
//        } catch (RemoteException e) {
//            LOG.debug("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
//    }

    // For version 3.8.0
    pointcut ping():
            withincode(void org.apache.zookeeper.server.quorum.Leader.lead()) &&
                    call(void org.apache.zookeeper.server.quorum.LearnerHandler.ping());

    void around(): ping() {
        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();
        LOG.debug("---------Before ping. Intercept leader judging isRunning. Subnode: {}", quorumPeerSubnodeId);
        try {
            quorumPeerAspect.setSubnodeSending();
            final int judgingRunningPacketId = quorumPeerAspect.getTestingService()
                    .offerLocalEvent(quorumPeerSubnodeId,
                            SubnodeType.QUORUM_PEER, -1L, null, TestingDef.MessageType.leaderJudgingIsRunning);
            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(quorumPeerSubnodeId, judgingRunningPacketId);

            // Trick: set RECEIVING state here
            quorumPeerAspect.getTestingService().setReceivingState(quorumPeerSubnodeId);

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
