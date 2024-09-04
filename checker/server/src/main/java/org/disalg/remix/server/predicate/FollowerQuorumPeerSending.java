package org.disalg.remix.server.predicate;

import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.api.NodeState;
import org.disalg.remix.api.SubnodeType;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * For now this indicates FollowerProcessSyncMessage & FollowerProcessNEWLEADER event
 */
public class FollowerQuorumPeerSending implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(FollowerQuorumPeerSending.class);

    private final ReplayService replayService;

    private final int followerId;

    public FollowerQuorumPeerSending(final ReplayService replayService,
                                     final int followerId) {
        this.replayService = replayService;
        this.followerId = followerId;
    }

    @Override
    public boolean isTrue() {
        final NodeState nodeState = replayService.getNodeStates().get(followerId);
        if (NodeState.ONLINE.equals(nodeState)) {
            for (final Subnode subnode : replayService.getSubnodeSets().get(followerId)) {
                if (subnode.getSubnodeType().equals(SubnodeType.QUORUM_PEER)) {
                    return SubnodeState.SENDING.equals(subnode.getState());
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return " Follower " + followerId + " 's QuorumPeer subnode is sending";
    }
}
