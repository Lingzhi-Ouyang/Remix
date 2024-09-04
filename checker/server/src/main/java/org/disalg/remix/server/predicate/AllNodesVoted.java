package org.disalg.remix.server.predicate;

import org.disalg.remix.api.NodeState;
import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/***
 * Wait Predicate for the end of an execution.
 */
public class AllNodesVoted implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesVoted.class);

    private final ReplayService replayService;

    private final Set<Integer> participants;

    public AllNodesVoted(final ReplayService replayService) {
        this.replayService = replayService;
        this.participants = null;
    }

    public AllNodesVoted(final ReplayService replayService, final Set<Integer> participants) {
        this.replayService = replayService;
        this.participants = participants;
    }

    @Override
    public boolean isTrue() {
        if (replayService.getSchedulingStrategy().hasNextEvent()) {
            LOG.debug("new event arrives");
            return true;
        }
        if ( participants != null) {
            for (Integer nodeId: participants) {
                LOG.debug("nodeId: {}, state: {}, votes: {}", nodeId, replayService.getNodeStates().get(nodeId), replayService.getVotes().get(nodeId));
                if (!NodeState.OFFLINE.equals(replayService.getNodeStates().get(nodeId))
                        && (!NodeState.ONLINE.equals(replayService.getNodeStates().get(nodeId)) || replayService.getVotes().get(nodeId) == null)) {
                    return false;
                }
            }
        } else {
            for (int nodeId = 0; nodeId < replayService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
                LOG.debug("nodeId: {}, state: {}, votes: {}", nodeId, replayService.getNodeStates().get(nodeId), replayService.getVotes().get(nodeId));
                if (!NodeState.OFFLINE.equals(replayService.getNodeStates().get(nodeId))
                        && (!NodeState.ONLINE.equals(replayService.getNodeStates().get(nodeId)) || replayService.getVotes().get(nodeId) == null)) {
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
