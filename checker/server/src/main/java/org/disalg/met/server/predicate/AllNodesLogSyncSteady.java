package org.disalg.met.server.predicate;

import org.disalg.met.api.SubnodeState;
import org.disalg.met.server.TestingService;
import org.disalg.met.api.NodeState;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for client request event when
 * - learnerHandlerSender is not intercepted
 * - syncProcessor is intercepted
 */
public class AllNodesLogSyncSteady implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesLogSyncSteady.class);

    private final TestingService testingService;

    public AllNodesLogSyncSteady(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean isTrue() {
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = testingService.getNodeStates().get(nodeId);
            if (NodeState.STARTING.equals(nodeState) || NodeState.STOPPING.equals(nodeState)) {
                LOG.debug("------Not steady-----Node {} status: {}",
                        nodeId, nodeState);
                return false;
            }
            else {
                LOG.debug("-----------Node {} status: {}",
                        nodeId, nodeState);
            }
            for (final Subnode subnode : testingService.getSubnodeSets().get(nodeId)) {
                if (SubnodeType.SYNC_PROCESSOR.equals(subnode.getSubnodeType()) &&
                        !SubnodeState.SENDING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for sync thread-----" +
                            "Node {} subnode {} status: {}, subnode type: {}",
                            nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                    return false;
                } else if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for other thread-----" +
                                    "Node {} subnode {} status: {}, subnode type: {}",
                            nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                    return false;
                } else {
                    LOG.debug("-----------Node {} status: {}, subnode {} status: {}, subnode type: {}",
                            nodeId, nodeState, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "allNodesLogSyncSteady";
    }
}