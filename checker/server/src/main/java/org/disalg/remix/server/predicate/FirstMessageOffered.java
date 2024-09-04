package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the first message of each node during election
 */
public class FirstMessageOffered implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(FirstMessageOffered.class);

    private final ReplayService replayService;

    private final int nodeId;

    public FirstMessageOffered(final ReplayService replayService, int nodeId) {
        this.replayService = replayService;
        this.nodeId = nodeId;
    }

    @Override
    public boolean isTrue() {
        return replayService.getFirstMessage().get(nodeId) != null;
    }

    @Override
    public String describe() {
        return "first message from node " + nodeId;
    }
}
