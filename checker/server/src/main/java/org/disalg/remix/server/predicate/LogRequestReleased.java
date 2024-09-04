package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.disalg.remix.api.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the release of a log request during election
 */
public class LogRequestReleased implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(LogRequestReleased.class);

    private final ReplayService replayService;

    private final int msgId;
    private final int syncNodeId;

    public LogRequestReleased(final ReplayService replayService, int msgId, int sendingNodeId) {
        this.replayService = replayService;
        this.msgId = msgId;
        this.syncNodeId = sendingNodeId;
    }

    @Override
    public boolean isTrue() {
        return replayService.getLogRequestInFlight() == msgId
                || NodeState.STOPPING.equals(replayService.getNodeStates().get(syncNodeId));
    }

    @Override
    public String describe() {
        return "release of log request " + msgId + " by node " + syncNodeId;
    }
}