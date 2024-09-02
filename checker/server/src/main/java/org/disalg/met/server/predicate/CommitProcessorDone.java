package org.disalg.met.server.predicate;

import org.disalg.met.api.NodeState;
import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitProcessorDone implements WaitPredicate{

    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessorDone.class);

    private final TestingService testingService;

    private final int msgId;
    private final int nodeId;
    private final long lastZxid;

    public CommitProcessorDone(final TestingService testingService,final int msgId, final int nodeId, final long lastZxid) {
        this.testingService = testingService;
        this.msgId = msgId;
        this.nodeId = nodeId;
        this.lastZxid = lastZxid;
    }

    @Override
    public boolean isTrue() {
        return testingService.getLastProcessedZxid(nodeId) > lastZxid
                || NodeState.STOPPING.equals(testingService.getNodeStates().get(nodeId));
    }

    @Override
    public String describe() {
        return " commit request " + msgId + " by node " + nodeId + " done";
    }
}
