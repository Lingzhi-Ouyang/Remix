package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class PrimeConnectionDone implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(PrimeConnectionDone.class);

    private final TestingService testingService;

    private final int clientId;

    public PrimeConnectionDone(final TestingService testingService, final int clientId) {
        this.testingService = testingService;
        this.clientId = clientId;
    }

    @Override
    public boolean isTrue() {
        return testingService.getClientProxy(clientId).isPrimeConnection();
    }

    @Override
    public String describe() {
        return " Prime Connection Done";
    }
}
