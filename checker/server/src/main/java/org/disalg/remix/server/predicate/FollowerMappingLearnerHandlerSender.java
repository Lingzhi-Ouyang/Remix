package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerMappingLearnerHandlerSender implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(FollowerMappingLearnerHandlerSender.class);

    private final ReplayService replayService;

    private final int nodeId;

    public FollowerMappingLearnerHandlerSender(final ReplayService replayService, final int nodeId) {
        this.replayService = replayService;
        this.nodeId = nodeId;
    }

    @Override
    public boolean isTrue() {
        return replayService.getFollowerLearnerHandlerSenderMap(nodeId) != null;
    }

    @Override
    public String describe() {
        return " follower " + nodeId + "  mapping its learnerHandlerSender";
    }
}
