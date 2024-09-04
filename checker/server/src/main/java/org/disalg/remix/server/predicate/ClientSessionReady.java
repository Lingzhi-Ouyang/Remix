package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ClientSessionReady implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(ClientSessionReady.class);

    private final ReplayService replayService;

    private final int clientId;

    public ClientSessionReady(final ReplayService replayService, final int clientId) {
        this.replayService = replayService;
        this.clientId = clientId;
    }

    @Override
    public boolean isTrue() {
        // all participants keep the same zxid after session initialization ( createSession & createKey all committed)
        Set<Integer> participants = replayService.getParticipants();
        long lastProcessedZxid = -1L;
        for (Integer peer: participants) {
            if (lastProcessedZxid < 0) {
                lastProcessedZxid = replayService.getLastProcessedZxid(peer);
            } else if (lastProcessedZxid != replayService.getLastProcessedZxid(peer)){
                return false;
            }
        }
        return replayService.getClientProxy(clientId).isReady();
    }

    @Override
    public String describe() {
        return "client session ready";
    }
}
