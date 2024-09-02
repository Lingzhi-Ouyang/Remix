package org.disalg.met.server.executor;

import org.disalg.met.api.SubnodeState;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.TestingDef;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.ElectionMessageEvent;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElectionMessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ElectionMessageExecutor.class);

    private final TestingService testingService;

    public ElectionMessageExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final ElectionMessageEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing message: {}", event.toString());
        releaseMessage(event);
        testingService.getControlMonitor().notifyAll();
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Message executed: {}\n\n\n", event.toString());
        return true;
    }

    // Should be called while holding a lock on controlMonitor
    /***
     * For leader election
     * set sendingSubnode and receivingSubnode WORKER_RECEIVER to PROCESSING
     * @param event
     */
    public void releaseMessage(final ElectionMessageEvent event) {
        testingService.setMessageInFlight(event.getId());
        final Subnode sendingSubnode = testingService.getSubnodes().get(event.getSendingSubnodeId());

        // set the sending subnode to be PROCESSING
        sendingSubnode.setState(SubnodeState.PROCESSING);

        if (event.getFlag() == TestingDef.RetCode.EXIT) {
            return;
        }

        // if in partition, then just drop it
        final int sendingNodeId = sendingSubnode.getNodeId();
        final int receivingNodeId = event.getReceivingNodeId();
        if (testingService.getPartitionMap().get(sendingNodeId).get(receivingNodeId)
                || event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION) {
            return;
        }

        // not in partition, so the message can be received
        // set the receiving subnode to be PROCESSING
        for (final Subnode subnode : testingService.getSubnodeSets().get(event.getReceivingNodeId())) {
            // ATTENTION: this is only for election
            if (subnode.getSubnodeType() == SubnodeType.WORKER_RECEIVER
                    && SubnodeState.RECEIVING.equals(subnode.getState())) {
                // set the receiving subnode to be PROCESSING
                subnode.setState(SubnodeState.PROCESSING);
                break;
            }
        }
    }
}
