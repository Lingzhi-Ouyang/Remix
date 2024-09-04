package org.disalg.remix.server.executor;

import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.event.NodeCrashEvent;
import org.disalg.remix.server.event.NodeStartEvent;

import java.io.IOException;

public class NodeCrashExecutor extends BaseEventExecutor {

    private final ReplayService replayService;

    private int crashBudget;

    public NodeCrashExecutor(final ReplayService replayService, final int crashBudget) {
        this.replayService = replayService;
        this.crashBudget = crashBudget;
    }

    @Override
    public boolean execute(final NodeCrashEvent event) throws IOException {
        boolean truelyExecuted = false;
        if (hasCrashes() || event.hasLabel()) {
            final int nodeId = event.getNodeId();
            if (!event.hasLabel()) {
                decrementCrashes();
            }
            if (replayService.getNodeStartExecutor().hasReboots()) {
                final NodeStartEvent nodeStartEvent = new NodeStartEvent(replayService.generateEventId(), nodeId, replayService.getNodeStartExecutor());
                nodeStartEvent.addDirectPredecessor(event);
                replayService.addEvent(nodeStartEvent);
            }
            replayService.stopNode(nodeId);
            replayService.getControlMonitor().notifyAll();
            replayService.waitAllNodesSteady();
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean hasCrashes() {
        return crashBudget > 0;
    }

    public void decrementCrashes() {
        crashBudget--;
    }
}
