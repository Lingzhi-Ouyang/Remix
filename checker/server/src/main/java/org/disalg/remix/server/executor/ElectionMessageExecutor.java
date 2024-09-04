package org.disalg.remix.server.executor;

import org.disalg.remix.api.MetaDef;
import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.api.SubnodeType;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.event.ElectionMessageEvent;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElectionMessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ElectionMessageExecutor.class);

    private final ReplayService replayService;

    public ElectionMessageExecutor(final ReplayService replayService) {
        this.replayService = replayService;
    }

    @Override
    public boolean execute(final ElectionMessageEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing message: {}", event.toString());
        releaseMessage(event);
        replayService.getControlMonitor().notifyAll();
        replayService.waitAllNodesSteady();
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
        replayService.setMessageInFlight(event.getId());
        final Subnode sendingSubnode = replayService.getSubnodes().get(event.getSendingSubnodeId());

        // set the sending subnode to be PROCESSING
        sendingSubnode.setState(SubnodeState.PROCESSING);

        if (event.getFlag() == MetaDef.RetCode.EXIT) {
            return;
        }

        // if in partition, then just drop it
        final int sendingNodeId = sendingSubnode.getNodeId();
        final int receivingNodeId = event.getReceivingNodeId();
        if (replayService.getPartitionMap().get(sendingNodeId).get(receivingNodeId)
                || event.getFlag() == MetaDef.RetCode.NODE_PAIR_IN_PARTITION) {
            return;
        }

        // not in partition, so the message can be received
        // set the receiving subnode to be PROCESSING
        for (final Subnode subnode : replayService.getSubnodeSets().get(event.getReceivingNodeId())) {
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
