package org.disalg.remix.server.predicate;

import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.api.NodeState;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for each executed event during election
 */
public class AllNodesSteady implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteady.class);

    private ReplayService replayService;

    public AllNodesSteady(final ReplayService replayService) {
        this.replayService = replayService;
    }

    @Override
    public boolean isTrue() {
        for (int nodeId = 0; nodeId < replayService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = replayService.getNodeStates().get(nodeId);
            if (NodeState.STARTING.equals(nodeState) || NodeState.STOPPING.equals(nodeState)) {
                LOG.debug("------Not steady-----Node {} status: {}\n",
                        nodeId, nodeState);
                return false;
            }
            else {
                LOG.debug("-----------Node {} status: {}",
                        nodeId, nodeState);
            }
            for (final Subnode subnode : replayService.getSubnodeSets().get(nodeId)) {
                if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                    LOG.debug("------Not steady-----Node {} subnode {} status: {}, subnode type: {}\n",
                            nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                    return false;
                }
                else {
                    LOG.debug("-----------Node {} subnode {} status: {}, subnode type: {}",
                            nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "allNodesSteady";
    }
}
