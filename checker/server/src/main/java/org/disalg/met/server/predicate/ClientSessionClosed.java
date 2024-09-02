package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSessionClosed implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(ClientSessionClosed.class);

    private final TestingService testingService;

    private final int clientId;

    public ClientSessionClosed(final TestingService testingService, int clientId) {
        this.testingService = testingService;
        this.clientId = clientId;
    }

    @Override
    public boolean isTrue() {
        return testingService.getClientProxy(clientId).isDone();
    }

    @Override
    public String describe() {
        return " client " + clientId + " closed";
    }
}
