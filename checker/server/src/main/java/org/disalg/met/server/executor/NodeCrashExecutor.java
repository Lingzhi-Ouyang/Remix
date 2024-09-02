package org.disalg.met.server.executor;

import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.NodeCrashEvent;
import org.disalg.met.server.event.NodeStartEvent;

import java.io.IOException;

public class NodeCrashExecutor extends BaseEventExecutor {

    private final TestingService testingService;

    private int crashBudget;

    public NodeCrashExecutor(final TestingService testingService, final int crashBudget) {
        this.testingService = testingService;
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
            if (testingService.getNodeStartExecutor().hasReboots()) {
                final NodeStartEvent nodeStartEvent = new NodeStartEvent(testingService.generateEventId(), nodeId, testingService.getNodeStartExecutor());
                nodeStartEvent.addDirectPredecessor(event);
                testingService.addEvent(nodeStartEvent);
            }
            testingService.stopNode(nodeId);
            testingService.getControlMonitor().notifyAll();
            testingService.waitAllNodesSteady();
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
