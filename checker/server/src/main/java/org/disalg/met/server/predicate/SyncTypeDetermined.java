package org.disalg.met.server.predicate;

import org.disalg.met.api.NodeState;
import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncTypeDetermined implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(SyncTypeDetermined.class);

    private final TestingService testingService;

    private final int syncNodeId;

    public SyncTypeDetermined(final TestingService testingService, int syncNodeId) {
        this.testingService = testingService;
        this.syncNodeId = syncNodeId;
    }

    @Override
    public boolean isTrue() {
        return testingService.getSyncType(syncNodeId) > 0;
    }

    @Override
    public String describe() {
        return "Sync Type of " + syncNodeId + " is determined: " + testingService.getSyncType(syncNodeId);
    }
}
