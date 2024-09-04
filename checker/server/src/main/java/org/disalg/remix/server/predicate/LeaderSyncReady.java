package org.disalg.remix.server.predicate;

import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LeaderSyncReady implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderSyncReady.class);

    private final ReplayService replayService;

    private final int leaderId;
    private final List<Integer> peers;

    public LeaderSyncReady(final ReplayService replayService,
                           final int leaderId,
                           final List<Integer> peers) {
        this.replayService = replayService;
        this.leaderId = leaderId;
        this.peers = peers;
    }

    @Override
    public boolean isTrue() {
//        // check if the follower's corresponding learner handler thread exists
//        if (replayService.getLeaderSyncFollowerCountMap().get(leaderId) != 0) {
//            return false;
//        }
        List<Integer> followerLearnerHandlerMap = replayService.getFollowerLearnerHandlerMap();

        for (Integer peer: peers) {
            final Integer subnodeId = followerLearnerHandlerMap.get(peer);
            if (subnodeId == null) return false;
            Subnode subnode = replayService.getSubnodes().get(subnodeId);
            if (!subnode.getState().equals(SubnodeState.SENDING)) {
                return false;
            }
        }
        return true;


//        // check if the follower's quorum peer thread is in SENDING state (SENDING ACKEPOCH)
//        assert peers != null;
//        for (Integer nodeId: peers) {
//            // check node state
//            final NodeState nodeState = replayService.getNodeStates().get(nodeId);
//            if (NodeState.STARTING.equals(nodeState) || NodeState.STOPPING.equals(nodeState)) {
//                LOG.debug("------follower {} not steady to sync, status: {}\n", nodeId, nodeState);
//                return false;
//            }
//            else {
//                LOG.debug("-----------follower {} status: {}", nodeId, nodeState);
//            }
//            // check subnode state
//            for (final Subnode subnode : replayService.getSubnodeSets().get(nodeId)) {
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
