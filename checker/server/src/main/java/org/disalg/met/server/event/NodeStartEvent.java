package org.disalg.met.server.event;

import org.disalg.met.server.executor.NodeStartExecutor;

import java.io.IOException;

public class NodeStartEvent extends AbstractEvent {

    private final int nodeId;

    public NodeStartEvent(final int id, final int nodeId, final NodeStartExecutor nodeStartExecutor) {
        super(id, nodeStartExecutor);
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
    public String toString() {
        return "NodeStartEvent{" +
                "id=" + getId() +
                ", nodeId=" + nodeId +
                getLabelString() +
                '}';
    }
}
