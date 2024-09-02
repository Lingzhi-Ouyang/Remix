package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

/***
 * ensure this is executed by the QuorumPeer thread
 */
public aspect FollowerZooKeeperServerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerZooKeeperServerAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    // intercept when a follower is about to log request
    pointcut followerLogRequest(Request r):
            withincode(* org.apache.zookeeper.server.quorum.FollowerZooKeeperServer.logRequest(..)) &&
                    call(void org.apache.zookeeper.server.SyncRequestProcessor.processRequest(Request)) && args(r);

    after(Request r) returning: followerLogRequest(r) {
        LOG.debug("----------followerLogRequest: {}", quorumPeerAspect.constructRequest(r));
        try {
            quorumPeerAspect.getTestingService().setReceivingState(quorumPeerAspect.getQuorumPeerSubnodeId());
        } catch (RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

}
