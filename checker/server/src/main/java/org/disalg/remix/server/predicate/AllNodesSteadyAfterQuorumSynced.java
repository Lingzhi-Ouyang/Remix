package org.disalg.remix.server.predicate;

import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.api.NodeState;
import org.disalg.remix.api.SubnodeType;
import org.disalg.remix.api.state.LeaderElectionState;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/***
 * This predicate is buggy. Do not use this.
 * Wait Predicate for client request event when
 * both learnerHandlerSender & commitProcessor are intercepted
 * - leader: COMMIT_PROCESSOR in SENDING state && LEARNER_HANDLER in SENDING states, other subnodes in SENDING / RECEIVING states
 * - follower: SYNC_PROCESSOR && FOLLOWER_PROCESSOR in SENDING / RECEIVING states
 */
public class AllNodesSteadyAfterQuorumSynced implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyAfterQuorumSynced.class);

    private final ReplayService replayService;

    private final Set<Integer> participants;

    public AllNodesSteadyAfterQuorumSynced(final ReplayService replayService) {
        this.replayService = replayService;
        this.participants = null;
    }

    public AllNodesSteadyAfterQuorumSynced(final ReplayService replayService, final Set<Integer> participants) {
        this.replayService = replayService;
        this.participants = participants;
    }

    @Override
    public boolean isTrue() {
        if (participants != null) {
            for (int nodeId: participants) {
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
                if (LeaderElectionState.LEADING.equals(leaderElectionState)) {
                    if (!leaderSteadyAfterQuorumSynced(nodeId)) {
                        return false;
                    }
                } else if (LeaderElectionState.FOLLOWING.equals(leaderElectionState)) {
                    if (!followerSteadyAfterQuorumSynced(nodeId)) {
                        return false;
                    }
                }
            }
        } else {
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
                if (LeaderElectionState.LEADING.equals(leaderElectionState)) {
                    if (!leaderSteadyAfterQuorumSynced(nodeId)) {
                        return false;
                    }
                } else if (LeaderElectionState.FOLLOWING.equals(leaderElectionState)) {
                    if (!followerSteadyAfterQuorumSynced(nodeId)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "all nodes steady after quorum have synced";
    }

    // is it needed to consider partition?
    private boolean leaderSteadyAfterQuorumSynced(final int nodeId) {
        boolean syncProcessorExisted = false;
        boolean commitProcessorExisted = false;
        // Note: learnerHandlerSender is created by learnerHandler so here we do not make a flag for learnerHandler
        boolean learnerHandlerSenderExisted = false;
        for (final Subnode subnode : replayService.getSubnodeSets().get(nodeId)) {
            if (SubnodeType.COMMIT_PROCESSOR.equals(subnode.getSubnodeType())) {
                syncProcessorExisted = true;
                if (!SubnodeState.SENDING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for leader's {} thread-----" +
                                    "Node {} subnode {} status: {}\n",
                            subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                    return false;
                }
            } else if (SubnodeType.LEARNER_HANDLER_SENDER.equals(subnode.getSubnodeType())) {
                learnerHandlerSenderExisted = true;
                if (!SubnodeState.SENDING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for leader's {} thread-----" +
                                    "Node {} subnode {} status: {}\n",
                            subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                    return false;
                }
            } else if (SubnodeType.SYNC_PROCESSOR.equals(subnode.getSubnodeType())) {
                commitProcessorExisted = true;
                if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for leader's {} thread-----" +
                                    "Node {} subnode {} status: {}\n",
                            subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                    return false;
                }
            }else if (SubnodeState.PROCESSING.equals(subnode.getState())) {
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

    private boolean followerSteadyAfterQuorumSynced(final int nodeId) {
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
        return syncProcessorExisted && commitProcessorExisted ;
//        return syncProcessorExisted && commitProcessorExisted && followerProcessorExisted;
    }
}
