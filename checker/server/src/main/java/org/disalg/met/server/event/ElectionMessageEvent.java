package org.disalg.met.server.event;

import org.disalg.met.api.TestingDef;
import org.disalg.met.server.executor.ElectionMessageExecutor;

import java.io.IOException;

public class ElectionMessageEvent extends AbstractEvent {

    private final int sendingSubnodeId;
    private final int receivingNodeId;
    private final String payload;
    private final long electionEpoch;
    private final int proposedLeader;

    public ElectionMessageEvent(final int id,
                                final int sendingSubnodeId,
                                final int receivingNodeId,
                                final long electionEpoch,
                                final int proposedLeader,
                                final String payload,
                                final ElectionMessageExecutor electionMessageExecutor) {
        super(id, electionMessageExecutor);
        this.sendingSubnodeId = sendingSubnodeId;
        this.receivingNodeId = receivingNodeId;
        this.payload = payload;
        this.electionEpoch = electionEpoch;
        this.proposedLeader = proposedLeader;
    }

    public int getSendingSubnodeId() {
        return sendingSubnodeId;
    }

    public int getReceivingNodeId() {
        return receivingNodeId;
    }

    public long getElectionEpoch() {
        return electionEpoch;
    }

    public int getProposedLeader() {
        return proposedLeader;
    }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public String toString() {
        return "ElectionMessageEvent{" +
                "id=" + getId() +
                ", flag=" + getFlag() +
                ", sendingSubnodeId=" + sendingSubnodeId +
                ", predecessors=" + getDirectPredecessorsString() +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
