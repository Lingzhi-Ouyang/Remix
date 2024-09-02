package org.disalg.met.server.predicate;

import org.disalg.met.api.NodeState;
import org.disalg.met.api.NodeStateForClientRequest;
import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the end of a client mutation event
 */
public class AllNodesSteadyBeforeMutation implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyBeforeMutation.class);

    private final TestingService testingService;

    public AllNodesSteadyBeforeMutation(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean isTrue() {
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = testingService.getNodeStates().get(nodeId);
            if (NodeState.STARTING.equals(nodeState) || NodeState.STOPPING.equals(nodeState) ) {
                LOG.debug("------not steady-----Node {} status: {}\n",
                        nodeId, nodeState);
                return false;
            }
            final NodeStateForClientRequest nodeStateForClientRequest
                    = testingService.getNodeStateForClientRequests(nodeId);
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
