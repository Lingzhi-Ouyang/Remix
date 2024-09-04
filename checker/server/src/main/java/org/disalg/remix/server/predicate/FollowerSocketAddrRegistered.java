package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerSocketAddrRegistered implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerSocketAddrRegistered.class);

    private final ReplayService replayService;

    private final String addr;

    public FollowerSocketAddrRegistered(final ReplayService replayService, String addr) {
        this.replayService = replayService;
        this.addr = addr;
    }

    @Override
    public boolean isTrue() {
        return replayService.getFollowerSocketAddressBook().contains(addr);
    }

    @Override
    public String describe() {
        return " follower socket address " + addr + " registered";
    }
}
