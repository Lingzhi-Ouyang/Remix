package org.disalg.remix.server.predicate;

import org.disalg.remix.api.NodeState;
import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitProcessorDone implements WaitPredicate{

    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessorDone.class);

    private final ReplayService replayService;

    private final int msgId;
    private final int nodeId;
    private final long lastZxid;

    public CommitProcessorDone(final ReplayService replayService, final int msgId, final int nodeId, final long lastZxid) {
        this.replayService = replayService;
        this.msgId = msgId;
        this.nodeId = nodeId;
        this.lastZxid = lastZxid;
    }

    @Override
    public boolean isTrue() {
        return replayService.getLastProcessedZxid(nodeId) > lastZxid
                || NodeState.STOPPING.equals(replayService.getNodeStates().get(nodeId));
    }

    @Override
    public String describe() {
        return " commit request " + msgId + " by node " + nodeId + " done";
    }
}
