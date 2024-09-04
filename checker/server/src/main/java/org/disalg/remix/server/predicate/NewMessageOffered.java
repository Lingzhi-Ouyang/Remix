package org.disalg.remix.server.predicate;

import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.api.NodeState;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the first message of each node after election
 *
 */
public class NewMessageOffered implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(NewMessageOffered.class);

    private final ReplayService replayService;

    public NewMessageOffered(final ReplayService replayService) {
        this.replayService = replayService;
    }

    @Override
    public boolean isTrue() {
        // if there exists one node offering a new message
        for (int nodeId = 0; nodeId < replayService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = replayService.getNodeStates().get(nodeId);
            if (NodeState.ONLINE.equals(nodeState)) {
                for (final Subnode subnode : replayService.getSubnodeSets().get(nodeId)) {
                    if (SubnodeState.SENDING.equals(subnode.getState())) {
                        LOG.debug("------NewMessageOffered-----Node {} status: {}, subnode {} status: {}, is main receiver : {}",
                                nodeId, nodeState, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                        return true;
                    }
                    LOG.debug("-----------Node {} status: {}, subnode {} status: {}, is main receiver : {}",
                            nodeId, nodeState, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                }
            }
        }
        LOG.debug("------New message has not yet come-----");
        return false;
    }

    @Override
    public String describe() {
        return "newMessageOffered";
    }
}
