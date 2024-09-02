package org.disalg.met.server.predicate;

import org.disalg.met.api.SubnodeState;
import org.disalg.met.api.NodeState;
import org.disalg.met.api.state.LeaderElectionState;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class AliveNodesInLookingState implements WaitPredicate{

    private static final Logger LOG = LoggerFactory.getLogger(AliveNodesInLookingState.class);

    private final TestingService testingService;

    private final Set<Integer> participants;

    public AliveNodesInLookingState(final TestingService testingService) {
        this.testingService = testingService;
        participants = new HashSet<>();
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            participants.add(nodeId);
        }
    }

    public AliveNodesInLookingState(final TestingService testingService, final Set<Integer> participants) {
        this.testingService = testingService;
        this.participants = participants;
    }



    @Override
    public boolean isTrue() {
        if (participants != null) {
            // broadcast events should be released first
            // o.w. the node will be blocked and will not return to LOOKING state
            LOG.debug("Try to release intercepted broadcast event first before the node get into LOOKING...");
            LOG.debug("Looking participants including : {}", participants);
            testingService.releaseBroadcastEvent(participants, true);
            for (Integer nodeId : participants) {
                if (checkNodeNotLooking(nodeId)) return false;
            }
        }
        return true;
    }

    private boolean checkNodeNotLooking(Integer nodeId) {
        final NodeState nodeState = testingService.getNodeStates().get(nodeId);
        LeaderElectionState leaderElectionState = testingService.getLeaderElectionStates().get(nodeId);
        switch (nodeState) {
            case STARTING:
            case STOPPING:
                LOG.debug("------Not steady-----Node {} status: {}\n", nodeId, nodeState);
                return true;
            case ONLINE:
            case UNREADY:
                if (!LeaderElectionState.LOOKING.equals(leaderElectionState)) {
                    LOG.debug("------Not steady-----Node {} status: {}, leaderElectionState: {}\n",
                            nodeId, nodeState, leaderElectionState);
                    return true;
                }
                LOG.debug("-----------Node {} status: {}, leaderElectionState: {}\n", nodeId, nodeState, leaderElectionState);
                break;
            case OFFLINE:
                LOG.debug("-----------Node {} status: {}", nodeId, nodeState);
        }
        for (final Subnode subnode: testingService.getSubnodeSets().get(nodeId)) {
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                LOG.debug("------Not steady-----Node {} subnode {} status: {}, subnode type: {}\n",
                        nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                return true;
            }
            else {
                LOG.debug("-----------Node {} subnode {} status: {}, subnode type: {}",
                        nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
            }
        }
        return false;
    }

    @Override
    public String describe() {
        if (participants == null) return "all nodes in LOOKING state";
        else return participants + " in LOOKING state";
    }
}
