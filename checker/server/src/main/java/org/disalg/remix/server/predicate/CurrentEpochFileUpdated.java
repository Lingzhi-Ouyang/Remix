package org.disalg.remix.server.predicate;

import org.disalg.remix.api.NodeState;
import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CurrentEpochFileUpdated implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(CurrentEpochFileUpdated.class);

    private final ReplayService replayService;

    private final int nodeId;
    private final long acceptedEpoch;

    public CurrentEpochFileUpdated(final ReplayService replayService,
                                   final int nodeId,
                                   final long acceptedEpoch) {
        this.replayService = replayService;
        this.nodeId = nodeId;
        this.acceptedEpoch = acceptedEpoch;
    }

    @Override
    public boolean isTrue() {
        return replayService.getCurrentEpoch(nodeId) == acceptedEpoch
                || NodeState.STOPPING.equals(replayService.getNodeStates().get(nodeId));
    }

    @Override
    public String describe() {
        return "currentEpoch file of node " + nodeId + " updated";
    }
}
