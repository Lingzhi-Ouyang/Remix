package org.disalg.remix.server.executor;

import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.event.NodeCrashEvent;
import org.disalg.remix.server.event.NodeStartEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NodeStartExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(NodeStartExecutor.class);

    private final ReplayService replayService;

    private int rebootBudget;

    public NodeStartExecutor(final ReplayService replayService, final int rebootBudget) {
        this.replayService = replayService;
        this.rebootBudget = rebootBudget;
    }

    @Override
    public boolean execute(final NodeStartEvent event)  throws IOException {
        boolean truelyExecuted = false;
        if (hasReboots()) {
            final int nodeId = event.getNodeId();
            replayService.setLastNodeStartEvent(nodeId, event);
            replayService.startNode(nodeId);
            replayService.getControlMonitor().notifyAll();
            replayService.waitAllNodesSteady();
            rebootBudget--;
            if (replayService.getNodeCrashExecutor().hasCrashes()) {
                final NodeCrashEvent nodeCrashEvent = new NodeCrashEvent(replayService.generateEventId(), nodeId, replayService.getNodeCrashExecutor());
                nodeCrashEvent.addDirectPredecessor(event);
                replayService.addEvent(nodeCrashEvent);
            }
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean hasReboots() {
        return rebootBudget > 0;
    }
}
