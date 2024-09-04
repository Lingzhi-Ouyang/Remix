package org.apache.zookeeper.server.quorum;

import org.disalg.remix.api.MessageType;
import org.disalg.remix.api.MetaDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

public aspect FollowerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    //Since follower will always reply ACK type, so it is more useful to match its last package type
    private Map<Long, Integer> zxidTypeMap = new HashMap<>();

    /***
     * For follower's receiving packet from leader during BROADCAST.
     * This interceptor is to acquire lastReadType
     * Related code: Follower.java
     */
    @Deprecated
    pointcut readPacket(QuorumPacket packet):
            withincode(* org.apache.zookeeper.server.quorum.Follower.followLeader(..)) &&
                    call(void org.apache.zookeeper.server.quorum.Learner.readPacket(QuorumPacket)) && args(packet);

    after(QuorumPacket packet) returning: readPacket(packet) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("follower readPacket-------Thread: {}, {}------", threadId, threadName);

        final String payload = quorumPeerAspect.packetToString(packet);
        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();

        LOG.debug("---------readPacket: ({}). Subnode: {}", payload, quorumPeerSubnodeId);
        final int type =  packet.getType();
        if (type == MessageType.PROPOSAL || type == MessageType.COMMIT) {
            zxidTypeMap.put(packet.getZxid(), type);
        }

    }

    /***
     * For follower's writePacket In BROADCAST (partition will work on the process)
     * TO be confirmed: for now we only focus on SYNC_THREAD
     * Since follower will always reply ACK type, so it is more useful to match its last package type
     *  --> FollowerProcessPROPOSAL:  send ACK to PROPOSAL.
     *  --> FollowerProcessCOMMIT: will not send anything.
     *  lastReadType here must be: PROPOSAL
     * Related code: Learner.java
     */
    pointcut writePacketInBROADCAST(QuorumPacket packet, boolean flush):
            withincode(* org.apache.zookeeper.server.quorum.SendAckRequestProcessor.processRequest(..)) &&
                    call(void org.apache.zookeeper.server.quorum.Learner.writePacket(QuorumPacket, boolean)) &&
                    args(packet, flush);

    void around(QuorumPacket packet, boolean flush): writePacketInBROADCAST(packet, flush) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("follower writePacket In BROADCAST -------Thread: {}, {}------", threadId, threadName);

        final String payload = quorumPeerAspect.packetToString(packet);

        final int syncSubnodeId = quorumPeerAspect.getSyncSubnodeId();
        LOG.debug("---------writePacket: ({}). Subnode: {}", payload, syncSubnodeId);
        final Integer type =  packet == null ? null : packet.getType();
        final Long zxid = packet == null ? null : packet.getZxid();
        int previousType;
        if (type == MessageType.PING) {
            previousType = MessageType.PING;
        } else {
            try {
                previousType = zxidTypeMap.get(zxid);
            } catch (NullPointerException npe) {
                LOG.warn("---------previousType is null, previous packet might be in SYNC: {}. Subnode: {}", payload, syncSubnodeId);
                previousType = MessageType.PROPOSAL_IN_SYNC;
            }

        }
        LOG.debug("Follower is about to reply a message type={} to leader's previous message (type={})", type, previousType);
        // if lastReadType == -1, this means to send ACK that should be processed during SYNC.
        try {
            quorumPeerAspect.setSubnodeSending();
            LOG.debug(" before followerWritePacketId, lastReadType: {}", previousType);
            final int followerWritePacketId = quorumPeerAspect.getRemoteService().offerFollowerToLeaderMessage(syncSubnodeId, zxid, payload, previousType);
            LOG.debug("followerWritePacketId = {}", followerWritePacketId);
            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(syncSubnodeId, followerWritePacketId);

            // Trick: set RECEIVING state here
            quorumPeerAspect.getRemoteService().setReceivingState(syncSubnodeId);

            // to check if the partition happens
            if (followerWritePacketId == MetaDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message.");
            }

            proceed(packet, flush);
            return;
        } catch (RemoteException | InterruptedException e) {
            LOG.debug("Encountered a remote exception", e);
        }
    }


    /***
     * For follower's writePacket In BROADCAST (partition will work on the process)
     * TO be confirmed: for now we only focus on SYNC_THREAD
     * Since follower will always reply ACK type, so it is more useful to match its last package type
     *  --> FollowerProcessPROPOSAL:  send ACK to PROPOSAL.
     *  --> FollowerProcessCOMMIT: will not send anything.
     *  lastReadType here must be: PING
     * Related code: Learner.java
     */
    pointcut writePacketInPING(QuorumPacket packet, boolean flush):
            withincode(* org.apache.zookeeper.server.quorum.Learner.ping(..)) &&
                    call(void org.apache.zookeeper.server.quorum.Learner.writePacket(QuorumPacket, boolean)) &&
                    args(packet, flush);

    void around(QuorumPacket packet, boolean flush): writePacketInPING(packet, flush) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("follower writePacketInPING In BROADCAST -------Thread: {}, {}------", threadId, threadName);

        final String payload = quorumPeerAspect.packetToString(packet);

        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();
        LOG.debug("---------writePacket: ({}). Subnode: {}", payload, quorumPeerSubnodeId);
        final Integer type =  packet == null ? null : packet.getType();
        final Long zxid = packet == null ? null : packet.getZxid();
        LOG.debug("Follower is about to reply a message type={} to leader's previous message (type=PING)", type);
        try {
            quorumPeerAspect.setSubnodeSending();
            LOG.debug(" before followerWritePacketId in writePacketInPING");
            final int followerWritePacketId = quorumPeerAspect.getRemoteService()
                    .offerFollowerToLeaderMessage(quorumPeerSubnodeId, zxid, payload, MessageType.PING);
            LOG.debug("in writePacketInPING, followerWritePacketId = {}", followerWritePacketId);
            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(quorumPeerSubnodeId, followerWritePacketId);

            // Trick: set RECEIVING state here
            quorumPeerAspect.getRemoteService().setReceivingState(quorumPeerSubnodeId);

            // to check if the partition happens
            if (followerWritePacketId == MetaDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message.");
                throw new InterruptedException();
            }

            proceed(packet, flush);
            return;
        } catch (RemoteException | InterruptedException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

}
