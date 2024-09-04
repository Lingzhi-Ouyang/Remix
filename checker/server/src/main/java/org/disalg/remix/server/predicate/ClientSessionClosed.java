package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSessionClosed implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(ClientSessionClosed.class);

    private final ReplayService replayService;

    private final int clientId;

    public ClientSessionClosed(final ReplayService replayService, int clientId) {
        this.replayService = replayService;
        this.clientId = clientId;
    }

    @Override
    public boolean isTrue() {
        return replayService.getClientProxy(clientId).isDone();
    }

    @Override
    public String describe() {
        return " client " + clientId + " closed";
    }
}
