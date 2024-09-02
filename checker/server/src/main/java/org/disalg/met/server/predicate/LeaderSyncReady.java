package org.disalg.met.server.predicate;

import org.disalg.met.api.NodeState;
import org.disalg.met.api.SubnodeState;
import org.disalg.met.server.TestingService;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LeaderSyncReady implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderSyncReady.class);

    private final TestingService testingService;

    private final int leaderId;
    private final List<Integer> peers;

    public LeaderSyncReady(final TestingService testingService,
                           final int leaderId,
                           final List<Integer> peers) {
        this.testingService = testingService;
        this.leaderId = leaderId;
        this.peers = peers;
    }

    @Override
    public boolean isTrue() {
//        // check if the follower's corresponding learner handler thread exists
//        if (testingService.getLeaderSyncFollowerCountMap().get(leaderId) != 0) {
//            return false;
//        }
        List<Integer> followerLearnerHandlerMap = testingService.getFollowerLearnerHandlerMap();

        for (Integer peer: peers) {
            final Integer subnodeId = followerLearnerHandlerMap.get(peer);
            if (subnodeId == null) return false;
            Subnode subnode = testingService.getSubnodes().get(subnodeId);
            if (!subnode.getState().equals(SubnodeState.SENDING)) {
                return false;
            }
        }
        return true;


//        // check if the follower's quorum peer thread is in SENDING state (SENDING ACKEPOCH)
//        assert peers != null;
//        for (Integer nodeId: peers) {
//            // check node state
//            final NodeState nodeState = testingService.getNodeStates().get(nodeId);
//            if (NodeState.STARTING.equals(nodeState) || NodeState.STOPPING.equals(nodeState)) {
//                LOG.debug("------follower {} not steady to sync, status: {}\n", nodeId, nodeState);
//                return false;
//            }
//            else {
//                LOG.debug("-----------follower {} status: {}", nodeId, nodeState);
//            }
//            // check subnode state
//            for (final Subnode subnode : testingService.getSubnodeSets().get(nodeId)) {
//                if (subnode.getSubnodeType().equals(SubnodeType.QUORUM_PEER)) {
//                    if (!SubnodeState.SENDING.equals(subnode.getState())) {
//                        LOG.debug("------follower not steady to sync -----Node {} subnode {} status: {}, subnode type: {}\n",
//                                nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
//                        return false;
//                    }
//                } else if (SubnodeState.PROCESSING.equals(subnode.getState())) {
//                    LOG.debug("------follower not steady to sync -----Node {} subnode {} status: {}, subnode type: {}\n",
//                            nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
//                    return false;
//                } else {
//                    LOG.debug("-----------follower {} subnode {} status: {}, subnode type: {}",
//                            nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
//                }
//            }
//        }
//        return true;
    }

    @Override
    public String describe() {
        return " Leader " + leaderId + " sync ready with peers: " + peers;
    }

}
