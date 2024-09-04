package org.disalg.remix.server.predicate;

import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.api.NodeState;
import org.disalg.remix.server.TestingService;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubnodeInSendingState implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(SubnodeInSendingState.class);

    private final TestingService testingService;

    private final int subnodeId;

    public SubnodeInSendingState(final TestingService testingService,
                                     final int subnodeId) {
        this.testingService = testingService;
        this.subnodeId = subnodeId;
    }

    @Override
    public boolean isTrue() {
        final Subnode subnode = testingService.getSubnodes().get(subnodeId);
        final int nodeId = subnode.getNodeId();
        final NodeState nodeState = testingService.getNodeStates().get(nodeId);
        if (NodeState.ONLINE.equals(nodeState)) {
            return SubnodeState.SENDING.equals(subnode.getState());
        }
        LOG.debug("node {} is not ONLINE!", nodeId);
        return true;
    }

    @Override
    public String describe() {
        return " Subnode " + subnodeId + " is in sending state";
    }

}
