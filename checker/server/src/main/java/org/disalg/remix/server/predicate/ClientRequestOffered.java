package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.executor.ClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientRequestOffered implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRequestOffered.class);

    private final ReplayService replayService;

    private final int clientId;

    public ClientRequestOffered(final ReplayService replayService, int clientId) {
        this.replayService = replayService;
        this.clientId = clientId;
    }

    @Override
    public boolean isTrue() {
        final ClientProxy clientProxy = replayService.getClientProxy(clientId);
        return !clientProxy.getRequestQueue().isEmpty()
                || clientProxy.isStop();
    }

    @Override
    public String describe() {
        return " request queue of client " + clientId + " not empty";
    }
}
