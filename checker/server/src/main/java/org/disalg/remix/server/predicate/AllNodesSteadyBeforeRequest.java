package org.disalg.remix.server.predicate;

import org.disalg.remix.api.Phase;
import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.api.NodeState;
import org.disalg.remix.api.state.LeaderElectionState;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/***
 * Pre-condition for client requests
 * Use NodePhases?
 */
public class AllNodesSteadyBeforeRequest implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyBeforeRequest.class);

    private final ReplayService replayService;

    public AllNodesSteadyBeforeRequest(final ReplayService replayService) {
        this.replayService = replayService;
    }

    @Override
    public boolean isTrue() {
        LOG.debug("Try to release all alive nodes' intercepted broadcast event first before the node get into LOOKING...");
        Set<Integer> nodes = new HashSet<>();
        for (int nodeId = 0; nodeId < replayService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            if (replayService.getNodeStates().get(nodeId).equals(NodeState.ONLINE) &&
                    replayService.getNodePhases().get(nodeId).equals(Phase.BROADCAST)) {
                nodes.add(nodeId);
            }
        }
        replayService.releaseBroadcastEvent(nodes, false);
        for (int nodeId = 0; nodeId < replayService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = replayService.getNodeStates().get(nodeId);
            switch (nodeState) {
                case STARTING:
                case STOPPING:
                    LOG.debug("------Not steady-----Node {} status: {}\n", nodeId, nodeState);
                    return false;
                case OFFLINE:
                    LOG.debug("-----------Node {} status: {}", nodeId, nodeState);
                    continue;
                case ONLINE:
                    LOG.debug("-----------Node {} status: {}", nodeId, nodeState);
            }
            LeaderElectionState leaderElectionState = replayService.getLeaderElectionStates().get(nodeId);
            // TODO: LOOKING ???
            if (LeaderElectionState.LEADING.equals(leaderElectionState)) {
                if (!leaderSteadyBeforeRequest(nodeId)) {
                    return false;
                }
            } else if (LeaderElectionState.FOLLOWING.equals(leaderElectionState)) {
                if (!followerSteadyBeforeRequest(nodeId)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "all nodes steady before request";
    }

    private boolean leaderSteadyBeforeRequest(final int nodeId) {
//        boolean syncProcessorExisted = false;
//        boolean commitProcessorExisted = false;
//        // Note: learnerHandlerSender is created by learnerHandler so here we do not make a flag for learnerHandler
//        // Note: we assume that learner handlers for all online followers will appear successfully if one of them exists
//        boolean learnerHandlerSenderExisted = false;
//        for (final Subnode subnode : replayService.getSubnodeSets().get(nodeId)) {
//            if (SubnodeType.SYNC_PROCESSOR.equals(subnode.getSubnodeType())) {
//                syncProcessorExisted = true;
//            } else if (SubnodeType.COMMIT_PROCESSOR.equals(subnode.getSubnodeType())) {
//                commitProcessorExisted = true;
//            } else if (SubnodeType.LEARNER_HANDLER_SENDER.equals(subnode.getSubnodeType())) {
//                learnerHandlerSenderExisted = true;
//            }
//            LOG.debug("-----------Leader node {} subnode {} status: {}, subnode type: {}",
//                    nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
//        }
//        return syncProcessorExisted && commitProcessorExisted && learnerHandlerSenderExisted;

        boolean syncProcessorExisted = false;
        boolean commitProcessorExisted = false;
        boolean learnerHandlerSenderExisted = false;
        for (final Subnode subnode : replayService.getSubnodeSets().get(nodeId)) {
            switch (subnode.getSubnodeType()) {
                case SYNC_PROCESSOR:
                    syncProcessorExisted = true;
                    break;
                case COMMIT_PROCESSOR:
                    commitProcessorExisted = true;
                    break;
                case LEARNER_HANDLER_SENDER:
                    learnerHandlerSenderExisted = true;
                    break;
                default:
            }
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                LOG.debug("------Not steady for leader's {} thread-----" +
                                "Node {} subnode {} status: {}\n",
                        subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                return false;
            }
            LOG.debug("-----------Leader node {} subnode {} status: {}, subnode type: {}",
                    nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
        }
        return syncProcessorExisted && commitProcessorExisted && learnerHandlerSenderExisted;
    }

    private boolean followerSteadyBeforeRequest(final int nodeId) {
//        boolean syncProcessorExisted = false;
//        boolean commitProcessorExisted = false;
//        boolean followerProcessorExisted = false;
//        for (final Subnode subnode : replayService.getSubnodeSets().get(nodeId)) {
//            if (SubnodeType.SYNC_PROCESSOR.equals(subnode.getSubnodeType())) {
//                syncProcessorExisted = true;
//            } else if (SubnodeType.COMMIT_PROCESSOR.equals(subnode.getSubnodeType())) {
//                commitProcessorExisted = true;
//            } else if (SubnodeType.FOLLOWER_PROCESSOR.equals(subnode.getSubnodeType())) {
//                followerProcessorExisted = true;
//            }
//            LOG.debug("-----------Follower node {} subnode {} status: {}, subnode type: {}",
//                    nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
//        }
//        return syncProcessorExisted && commitProcessorExisted && followerProcessorExisted;
//    }

        boolean syncProcessorExisted = false;
        boolean commitProcessorExisted = false;
//        boolean followerProcessorExisted = false;
        for (final Subnode subnode : replayService.getSubnodeSets().get(nodeId)) {
            switch (subnode.getSubnodeType()) {
                case SYNC_PROCESSOR:
                    syncProcessorExisted = true;
                    break;
                case COMMIT_PROCESSOR:
                    commitProcessorExisted = true;
                    break;
//                case FOLLOWER_PROCESSOR:
//                    followerProcessorExisted = true;
//                    break;
                default:
            }
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                LOG.debug("------Not steady for follower's {} thread-----" +
                                "Node {} subnode {} status: {}\n",
                        subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                return false;
            }
            LOG.debug("-----------Follower node {} subnode {} status: {}, subnode type: {}",
                    nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
        }
//        return syncProcessorExisted && commitProcessorExisted && followerProcessorExisted;
        return syncProcessorExisted && commitProcessorExisted;
    }
}
