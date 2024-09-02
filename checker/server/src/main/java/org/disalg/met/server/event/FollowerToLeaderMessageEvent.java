package org.disalg.met.server.event;

import org.disalg.met.server.executor.FollowerToLeaderMessageExecutor;
import org.disalg.met.api.MessageType;

import java.io.IOException;

public class FollowerToLeaderMessageEvent extends AbstractEvent {
    private final int sendingSubnodeId;
    private final int receivingNodeId;
    private final String payload;
    private final int type; // this describes the message type that this ACK replies to
    private final long zxid;

    public FollowerToLeaderMessageEvent(final int id,
                                        final int sendingSubnodeId,
                                        final int receivingNodeId,
                                        final int type,
                                        final long zxid,
                                        final String payload,
                                        final FollowerToLeaderMessageExecutor messageExecutor) {
        super(id, messageExecutor);
        this.sendingSubnodeId = sendingSubnodeId;
        this.receivingNodeId = receivingNodeId;
        this.payload = payload;
        this.type = type;
        this.zxid = zxid;
    }

    public int getSendingSubnodeId() {
        return sendingSubnodeId;
    }

    public int getReceivingNodeId() {
        return receivingNodeId;
    }

    public int getType() {
        return type;
    }

    public long getZxid() {
        return zxid;
    }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public String toString() {
        String action = "FollowerSendACKto";
        switch (type) {
            case MessageType.LEADERINFO:
                action = "FollowerSendACKEPOCH";
                break;
            case MessageType.NEWLEADER:
                action += "NEWLEADER";
                break;
            case MessageType.UPTODATE:
                action += "UPTODATE";
                break;
            case MessageType.PROPOSAL:
                action += "PROPOSAL";
                break;
            case MessageType.PROPOSAL_IN_SYNC:
                action += "PROPOSAL_IN_SYNC";
                break;
        }
        return action + "{" +
                "id=" + getId() +
                ", flag=" + getFlag() +
                ", sendingSubnodeId=" + sendingSubnodeId +
                ", receivingNode=" + receivingNodeId +
                ", predecessors=" + getDirectPredecessorsString() +
                ", ackMessageType=" + type +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
