package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.disalg.met.server.executor.ClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientRequestOffered implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRequestOffered.class);

    private final TestingService testingService;

    private final int clientId;

    public ClientRequestOffered(final TestingService testingService, int clientId) {
        this.testingService = testingService;
        this.clientId = clientId;
    }

    @Override
    public boolean isTrue() {
        final ClientProxy clientProxy = testingService.getClientProxy(clientId);
        return !clientProxy.getRequestQueue().isEmpty()
                || clientProxy.isStop();
    }

    @Override
    public String describe() {
        return " request queue of client " + clientId + " not empty";
    }
}
