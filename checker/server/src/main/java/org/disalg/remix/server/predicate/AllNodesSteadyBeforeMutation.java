package org.disalg.remix.server.predicate;

import org.disalg.remix.api.NodeState;
import org.disalg.remix.api.NodeStateForClientRequest;
import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the end of a client mutation event
 */
public class AllNodesSteadyBeforeMutation implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyBeforeMutation.class);

    private final ReplayService replayService;

    public AllNodesSteadyBeforeMutation(final ReplayService replayService) {
        this.replayService = replayService;
    }

    @Override
    public boolean isTrue() {
        for (int nodeId = 0; nodeId < replayService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = replayService.getNodeStates().get(nodeId);
            if (NodeState.STARTING.equals(nodeState) || NodeState.STOPPING.equals(nodeState) ) {
                LOG.debug("------not steady-----Node {} status: {}\n",
                        nodeId, nodeState);
                return false;
            }
            final NodeStateForClientRequest nodeStateForClientRequest
                    = replayService.getNodeStateForClientRequests(nodeId);
            if ( NodeStateForClientRequest.SET_PROCESSING.equals(nodeStateForClientRequest)){
                LOG.debug("------not steady-----Node {} nodeStateForClientRequest: {}\n",
                        nodeId, nodeStateForClientRequest);
                return false;
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "AllNodesSteadyBeforeMutation";
    }
}
