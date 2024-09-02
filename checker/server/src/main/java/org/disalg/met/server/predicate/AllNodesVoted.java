package org.disalg.met.server.predicate;

import org.disalg.met.api.NodeState;
import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/***
 * Wait Predicate for the end of an execution.
 */
public class AllNodesVoted implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesVoted.class);

    private final TestingService testingService;

    private final Set<Integer> participants;

    public AllNodesVoted(final TestingService testingService) {
        this.testingService = testingService;
        this.participants = null;
    }

    public AllNodesVoted(final TestingService testingService, final Set<Integer> participants) {
        this.testingService = testingService;
        this.participants = participants;
    }

    @Override
    public boolean isTrue() {
        if (testingService.getSchedulingStrategy().hasNextEvent()) {
            LOG.debug("new event arrives");
            return true;
        }
        if ( participants != null) {
            for (Integer nodeId: participants) {
                LOG.debug("nodeId: {}, state: {}, votes: {}", nodeId, testingService.getNodeStates().get(nodeId), testingService.getVotes().get(nodeId));
                if (!NodeState.OFFLINE.equals(testingService.getNodeStates().get(nodeId))
                        && (!NodeState.ONLINE.equals(testingService.getNodeStates().get(nodeId)) || testingService.getVotes().get(nodeId) == null)) {
                    return false;
                }
            }
        } else {
            for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
                LOG.debug("nodeId: {}, state: {}, votes: {}", nodeId, testingService.getNodeStates().get(nodeId), testingService.getVotes().get(nodeId));
                if (!NodeState.OFFLINE.equals(testingService.getNodeStates().get(nodeId))
                        && (!NodeState.ONLINE.equals(testingService.getNodeStates().get(nodeId)) || testingService.getVotes().get(nodeId) == null)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        if (participants != null) return participants + " voted";
        else return "allNodesVoted";
    }
}
