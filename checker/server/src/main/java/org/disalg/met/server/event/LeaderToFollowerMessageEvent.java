package org.disalg.met.server.event;

import org.disalg.met.api.TestingDef;
import org.disalg.met.server.executor.LeaderToFollowerMessageExecutor;
import org.disalg.met.api.MessageType;

import java.io.IOException;

public class LeaderToFollowerMessageEvent extends AbstractEvent{
    private final int sendingSubnodeId;
    private final int receivingNodeId;
    private final String payload;
    private final int type;
    private final long zxid;

    public LeaderToFollowerMessageEvent(final int id,
                                        final int sendingSubnodeId,
                                        final int receivingNodeId,
                                        final int type,
                                        final long zxid,
                                        final String payload,
                                        final LeaderToFollowerMessageExecutor messageExecutor) {
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
        String action = "LeaderToFollowerMessageEvent";
        switch (type) {
            case MessageType.LEADERINFO:
                action = "LeaderSendLEADERINFO";
                break;
            case MessageType.DIFF:
                action = "LeaderSendDIFF";
                break;
            case MessageType.TRUNC:
                action = "LeaderSendTRUNC";
                break;
            case MessageType.SNAP:
                action = "LeaderSendSNAP";
                break;
            case MessageType.NEWLEADER:
                action = "LeaderSendNEWLEADER";
                break;
            case MessageType.UPTODATE:
                action = "LeaderSendUPTODATE";
                break;
            case MessageType.PROPOSAL:
                action = "LeaderSendPROPOSAL";
                break;
            case MessageType.COMMIT:
                action = "LeaderSendCOMMIT";
                break;
            case TestingDef.MessageType.learnerHandlerReadRecord:
                action = "LearnerHandlerReadRecord";
                break;
        }
        return action + "{" +
                "id=" + getId() +
                ", flag=" + getFlag() +
                ", sendingSubnodeId=" + sendingSubnodeId +
                ", receivingNodeId=" + receivingNodeId +
                ", predecessors=" + getDirectPredecessorsString() +
                ", type=" + type +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
