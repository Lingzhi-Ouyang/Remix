package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerMappingLearnerHandlerSender implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(FollowerMappingLearnerHandlerSender.class);

    private final TestingService testingService;

    private final int nodeId;

    public FollowerMappingLearnerHandlerSender(final TestingService testingService, final int nodeId) {
        this.testingService = testingService;
        this.nodeId = nodeId;
    }

    @Override
    public boolean isTrue() {
        return testingService.getFollowerLearnerHandlerSenderMap(nodeId) != null;
    }

    @Override
    public String describe() {
        return " follower " + nodeId + "  mapping its learnerHandlerSender";
    }
}
