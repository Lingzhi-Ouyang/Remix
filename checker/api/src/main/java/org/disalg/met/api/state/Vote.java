package org.disalg.met.api.state;

import java.io.Serializable;

public class Vote implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long leader;
    private final long zxid;
    private final long electionEpoch;
    private final long peerEpoch;

    public Vote(final long leader, final long zxid, final long electionEpoch, final long peerEpoch) {
        this.leader = leader;
        this.zxid = zxid;
        this.electionEpoch = electionEpoch;
        this.peerEpoch = peerEpoch;
    }

    public long getLeader() {
        return leader;
    }

    public long getZxid() {
        return zxid;
    }

    public long getElectionEpoch() {
        return electionEpoch;
    }

    public long getPeerEpoch() {
        return peerEpoch;
    }

    @Override
    public String toString() {
        return "Vote{" +
                "leader=" + leader +
                ", zxid=" + zxid +
                ", electionEpoch=" + electionEpoch +
                ", peerEpoch=" + peerEpoch +
                '}';
    }
}
