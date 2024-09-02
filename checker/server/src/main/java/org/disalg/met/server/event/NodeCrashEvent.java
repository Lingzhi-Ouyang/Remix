package org.disalg.met.server.event;

import org.disalg.met.server.executor.NodeCrashExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.prefs.NodeChangeEvent;

public class NodeCrashEvent extends AbstractEvent {

    private static final Logger LOG = LoggerFactory.getLogger(NodeChangeEvent.class);

    private final int nodeId;

    public NodeCrashEvent(final int id, final int nodeId, final NodeCrashExecutor nodeCrashExecutor) {
        super(id, nodeCrashExecutor);
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public void setLabel(int label) {
        super.setLabel(label);
        final NodeCrashExecutor nodeCrashExecutor = (NodeCrashExecutor) getEventExecutor();
        if (nodeCrashExecutor.hasCrashes()) {
            nodeCrashExecutor.decrementCrashes();
        }
        else {
            LOG.warn("Set a label on a NodeCrashEvent even though crash budget is empty: label={}, event={}", label, toString());
        }
    }

    @Override
    public String toString() {
        return "NodeCrashEvent{" +
                "id=" + getId() +
                ", nodeId=" + nodeId +
                getLabelString() +
                '}';
    }
}
