package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.disalg.met.api.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the release of a log request during election
 */
public class LogRequestReleased implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(LogRequestReleased.class);

    private final TestingService testingService;

    private final int msgId;
    private final int syncNodeId;

    public LogRequestReleased(final TestingService testingService, int msgId, int sendingNodeId) {
        this.testingService = testingService;
        this.msgId = msgId;
        this.syncNodeId = sendingNodeId;
    }

    @Override
    public boolean isTrue() {
        return testingService.getLogRequestInFlight() == msgId
                || NodeState.STOPPING.equals(testingService.getNodeStates().get(syncNodeId));
    }

    @Override
    public String describe() {
        return "release of log request " + msgId + " by node " + syncNodeId;
    }
}