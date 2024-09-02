package org.disalg.met.server.event;

import org.disalg.met.server.executor.PartitionStartExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PartitionStartEvent extends AbstractEvent {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionStartEvent.class);

    private final int node1;
    private final int node2;

    public PartitionStartEvent(final int id, final int node1, final int node2, final PartitionStartExecutor partitionStartExecutor) {
        super(id, partitionStartExecutor);
        this.node1 = node1;
        this.node2 = node2;
    }

    public int getNode1() {
        return node1;
    }

    public int getNode2() {
        return node2;
    }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }


    @Override
    public String toString() {
        return "PartitionStart{" +
                "id=" + getId() +
                ", start partition nodes: [" + node1 + ", " +node2 + "]" +
                getLabelString() +
                '}';
    }
}
