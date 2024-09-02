package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerSocketAddrRegistered implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerSocketAddrRegistered.class);

    private final TestingService testingService;

    private final String addr;

    public FollowerSocketAddrRegistered(final TestingService testingService, String addr) {
        this.testingService = testingService;
        this.addr = addr;
    }

    @Override
    public boolean isTrue() {
        return testingService.getFollowerSocketAddressBook().contains(addr);
    }

    @Override
    public String describe() {
        return " follower socket address " + addr + " registered";
    }
}
