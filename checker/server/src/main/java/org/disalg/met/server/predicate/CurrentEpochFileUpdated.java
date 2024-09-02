package org.disalg.met.server.predicate;

import org.disalg.met.api.NodeState;
import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CurrentEpochFileUpdated implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(CurrentEpochFileUpdated.class);

    private final TestingService testingService;

    private final int nodeId;
    private final long acceptedEpoch;

    public CurrentEpochFileUpdated(final TestingService testingService,
                                   final int nodeId,
                                   final long acceptedEpoch) {
        this.testingService = testingService;
        this.nodeId = nodeId;
        this.acceptedEpoch = acceptedEpoch;
    }

    @Override
    public boolean isTrue() {
        return testingService.getCurrentEpoch(nodeId) == acceptedEpoch
                || NodeState.STOPPING.equals(testingService.getNodeStates().get(nodeId));
    }

    @Override
    public String describe() {
        return "currentEpoch file of node " + nodeId + " updated";
    }
}
