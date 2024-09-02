package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ClientSessionReady implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(ClientSessionReady.class);

    private final TestingService testingService;

    private final int clientId;

    public ClientSessionReady(final TestingService testingService, final int clientId) {
        this.testingService = testingService;
        this.clientId = clientId;
    }

    @Override
    public boolean isTrue() {
        // all participants keep the same zxid after session initialization ( createSession & createKey all committed)
        Set<Integer> participants = testingService.getParticipants();
        long lastProcessedZxid = -1L;
        for (Integer peer: participants) {
            if (lastProcessedZxid < 0) {
                lastProcessedZxid = testingService.getLastProcessedZxid(peer);
            } else if (lastProcessedZxid != testingService.getLastProcessedZxid(peer)){
                return false;
            }
        }
        return testingService.getClientProxy(clientId).isReady();
    }

    @Override
    public String describe() {
        return "client session ready";
    }
}
