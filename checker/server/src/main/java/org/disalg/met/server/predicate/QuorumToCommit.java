package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumToCommit implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(QuorumToCommit.class);

    private final TestingService testingService;

    private final long zxid;

    private final int nodeNum;

    public QuorumToCommit(final TestingService testingService, final long zxid, final int nodeNum) {
        this.testingService = testingService;
        this.zxid = zxid;
        this.nodeNum = nodeNum;
    }

    @Override
    public boolean isTrue() {
        if (testingService.getZxidToCommitMap().containsKey(zxid)){
            final int count = testingService.getZxidToCommitMap().get(zxid);
            return count > nodeNum / 2;
        }
        return false;
    }

    @Override
    public String describe() {
        return "quorum nodes to commit zxid = 0x" + Long.toHexString(zxid);
    }
}
