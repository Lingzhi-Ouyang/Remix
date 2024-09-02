package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the first message of each node during election
 */
public class FirstMessageOffered implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(FirstMessageOffered.class);

    private final TestingService testingService;

    private final int nodeId;

    public FirstMessageOffered(final TestingService testingService, int nodeId) {
        this.testingService = testingService;
        this.nodeId = nodeId;
    }

    @Override
    public boolean isTrue() {
        return testingService.getFirstMessage().get(nodeId) != null;
    }

    @Override
    public String describe() {
        return "first message from node " + nodeId;
    }
}
