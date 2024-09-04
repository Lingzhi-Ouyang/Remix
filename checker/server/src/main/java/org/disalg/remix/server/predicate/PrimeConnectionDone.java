package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimeConnectionDone implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(PrimeConnectionDone.class);

    private final ReplayService replayService;

    private final int clientId;

    public PrimeConnectionDone(final ReplayService replayService, final int clientId) {
        this.replayService = replayService;
        this.clientId = clientId;
    }

    @Override
    public boolean isTrue() {
        return replayService.getClientProxy(clientId).isPrimeConnection();
    }

    @Override
    public String describe() {
        return " Prime Connection Done";
    }
}
