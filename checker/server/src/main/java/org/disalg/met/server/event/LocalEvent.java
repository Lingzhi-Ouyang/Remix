package org.disalg.met.server.event;

import org.disalg.met.api.MessageType;
import org.disalg.met.api.TestingDef;
import org.disalg.met.server.executor.LocalEventExecutor;
import org.disalg.met.api.SubnodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/***
 * This class describes the local event of a node, such as
 *  -> log proposal to disk
 *  -> commit
 *  -> follower process DIFF / TRUNC / SNAP
 */
public class LocalEvent extends AbstractEvent{
    private static final Logger LOG = LoggerFactory.getLogger(LocalEvent.class);

    private final int nodeId;
    private final int subnodeId;
    private final SubnodeType subnodeType; // QuorumPeer / SYNC / COMMIT
    private final String payload;
    private final long zxid;
    private final int type;  // ZooDefs.OpCode for SYNC / COMMIT , and messageType for QuorumPeer of follower

    public LocalEvent(final int id, final int nodeId, final int subnodeId, final SubnodeType subnodeType,
                      final String payload, final long zxid, final int type, final LocalEventExecutor localEventExecutor) {
        super(id, localEventExecutor);
        this.nodeId = nodeId;
        this.subnodeId = subnodeId;
        this.subnodeType = subnodeType;
        this.payload = payload;
        this.zxid = zxid;
        this.type = type;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getSubnodeId() { return subnodeId; }

    public SubnodeType getSubnodeType() {
        return subnodeType;
    }

    public String getPayload() {
        return payload;
    }

    public long getZxid() {
        return zxid;
    }

    public int getType() {
        return type;
    }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public String toString() {
        String action = "LocalEvent";
        if (subnodeType.equals(SubnodeType.SYNC_PROCESSOR)) {
            action = "LogRequest";
        } else if (subnodeType.equals(SubnodeType.COMMIT_PROCESSOR)) {
            action = "Commit";
        } else if (subnodeType.equals(SubnodeType.QUORUM_PEER)) {
            switch (type) {
                case MessageType.DIFF:
                    action = "FollowerProcessDIFF";
                    break;
                case MessageType.TRUNC:
                    action = "FollowerProcessTRUNC";
                    break;
                case MessageType.SNAP:
                    action = "FollowerProcessSNAP";
                    break;
                case MessageType.NEWLEADER:
                    // As a local event, it means that follower has updated its current epoch in zk-3.8 till Dec. 2022
                    action = "FollowerProcessNEWLEADERAfterCurrentEpochUpdated";
                    break;
                case MessageType.UPTODATE:
                    action = "FollowerProcessUPTODATE";
                    break;
                case MessageType.PROPOSAL:
                    action = "FollowerLogPROPOSAL";
                    break;
                case MessageType.COMMIT:
                    action = "FollowerProcessCOMMIT";
                    break;
                case TestingDef.MessageType.leaderJudgingIsRunning:
                    action = "LeaderJudgingIsRunning";
                    break;
            }
        } else if (subnodeType.equals(SubnodeType.LEARNER_HANDLER)) {
            action = "LeaderSyncFollower";
        }
        return action +
                "{id=" + getId() +
                ", flag=" + getFlag() +
                ", type=" + type +
                ", nodeId=" + nodeId +
                ", subnodeId=" + subnodeId +
                ", subnodeType=" + subnodeType +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
