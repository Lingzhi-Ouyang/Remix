package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncTypeDetermined implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(SyncTypeDetermined.class);

    private final ReplayService replayService;

    private final int syncNodeId;

    public SyncTypeDetermined(final ReplayService replayService, int syncNodeId) {
        this.replayService = replayService;
        this.syncNodeId = syncNodeId;
    }

    @Override
    public boolean isTrue() {
        return replayService.getSyncType(syncNodeId) > 0;
    }

    @Override
    public String describe() {
        return "Sync Type of " + syncNodeId + " is determined: " + replayService.getSyncType(syncNodeId);
    }
}
