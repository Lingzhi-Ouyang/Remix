package org.disalg.remix.server.predicate;

import org.disalg.remix.server.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumToCommit implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(QuorumToCommit.class);

    private final ReplayService replayService;

    private final long zxid;

    private final int nodeNum;

    public QuorumToCommit(final ReplayService replayService, final long zxid, final int nodeNum) {
        this.replayService = replayService;
        this.zxid = zxid;
        this.nodeNum = nodeNum;
    }

    @Override
    public boolean isTrue() {
        if (replayService.getZxidToCommitMap().containsKey(zxid)){
            final int count = replayService.getZxidToCommitMap().get(zxid);
            return count > nodeNum / 2;
        }
        return false;
    }

    @Override
    public String describe() {
        return "quorum nodes to commit zxid = 0x" + Long.toHexString(zxid);
    }
}
