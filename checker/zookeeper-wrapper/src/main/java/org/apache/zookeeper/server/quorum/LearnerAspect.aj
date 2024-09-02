package org.apache.zookeeper.server.quorum;

import org.disalg.met.api.MessageType;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.TestingDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;

/***
 * ensure this is executed by the QuorumPeer thread
 */
public aspect LearnerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    //Since follower will always reply ACK type, so it is more useful to match its last package type
    private int lastReadType = -1;

    /***
     * After Election and at the end of discovery, follower will send ACKEPOCH to leader
     * Only ACKEPOCH will be intercepted
     * Related code: Learner.java
     */
    pointcut writePacketInRegisterWithLeader(QuorumPacket packet, boolean flush):
            withincode(* org.apache.zookeeper.server.quorum.Learner.registerWithLeader(..)) &&
                    call(void org.apache.zookeeper.server.quorum.Learner.writePacket(QuorumPacket, boolean)) && args(packet, flush);

    void around(QuorumPacket packet, boolean flush): writePacketInRegisterWithLeader(packet, flush) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("follower writePacketInRegisterWithLeader-------Thread: {}, {}------", threadId, threadName);

        final String payload = quorumPeerAspect.packetToString(packet);
        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();
        LOG.debug("---------writePacket: ({}). Subnode: {}", payload, quorumPeerSubnodeId);

        final int type =  packet.getType();
        if (type != Leader.ACKEPOCH) {
            LOG.debug("Follower is about to reply a message to leader which is not an ACKEPOCH. (type={})", type);
            proceed(packet, flush);
            return;
        }

        try {
            lastReadType = Leader.LEADERINFO;
            quorumPeerAspect.setSubnodeSending();
            final long zxid = packet.getZxid();
            final int followerWritePacketId = quorumPeerAspect.getTestingService().offerFollowerToLeaderMessage(quorumPeerSubnodeId, zxid, payload, lastReadType);

            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(quorumPeerSubnodeId, followerWritePacketId);

            // Trick: set RECEIVING state here
            quorumPeerAspect.getTestingService().setReceivingState(quorumPeerSubnodeId);

            // to check if the partition happens
            if (followerWritePacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message.");
                throw new IOException();
//                return;
            }

            proceed(packet, flush);
            return;
        } catch (RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.debug("Encountered IOException", e);
            throw new RuntimeException(e);
        }
    }


    /***
     * For follower's sync with leader process without replying (partition will not work)
     * For getting lastReadType during SYNC:
     *  DIFF / TRUNC / SNAP --> FollowerProcessSyncMessage : will not send ACK.
     *  PROPOSAL --> FollowerProcessPROPOSALInSync :  will not send ACK during sync. Actually will send ACK until in broadcast phase. see zk-3911
     *  COMMIT --> FollowerProcessCOMMITInSync : will not send ACK.
     *  NEWLEADER --> record this message type, will send ACK, so will be processed in writePacketInSyncWithLeader
     *  UPTODATE --> record this message type, will send ACK, so will be processed in writePacketInSyncWithLeader
     * Related code: Learner.java
     */
    pointcut readPacketInSyncWithLeader(QuorumPacket packet):
            withincode(* org.apache.zookeeper.server.quorum.Learner.syncWithLeader(..)) &&
                    call(void org.apache.zookeeper.server.quorum.Learner.readPacket(QuorumPacket)) && args(packet);

    after(QuorumPacket packet) returning: readPacketInSyncWithLeader(packet) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("follower readPacketInSyncWithLeader-------Thread: {}, {}------", threadId, threadName);

        final String payload = quorumPeerAspect.packetToString(packet);
        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();

        // Set RECEIVING state since there is nowhere else to set
        try {
            quorumPeerAspect.getTestingService().setReceivingState(quorumPeerSubnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }


        LOG.debug("---------readPacket: ({}). Subnode: {}", payload, quorumPeerSubnodeId);
        final int type =  packet.getType();
        lastReadType = type;
    }

    /***
     * For follower's sync with leader process with sending REPLY (partition will work on the process)
     * Since follower will always reply ACK type, so it is more useful to match its last package type
     *  lastReadType==UPTODATE --> FollowerProcessUPTODATE : send ACK to UPTODATE, offerFollowerToLeaderMessage
     *  lastReadType==NEWLEADER --> FollowerProcessNEWLEADER : send ACK to NEWLEADER,  offerFollowerToLeaderMessage
     * Related code: Learner.java
     */
    pointcut writePacketInSyncWithLeader(QuorumPacket packet, boolean flush):
            withincode(* org.apache.zookeeper.server.quorum.Learner.syncWithLeader(..)) &&
                    call(void org.apache.zookeeper.server.quorum.Learner.writePacket(QuorumPacket, boolean)) && args(packet, flush);

    void around(QuorumPacket packet, boolean flush): writePacketInSyncWithLeader(packet, flush) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("follower writePacketInSyncWithLeader-------Thread: {}, {}------", threadId, threadName);

        final String payload = quorumPeerAspect.packetToString(packet);
        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();
        LOG.debug("---------writePacket: ({}). Subnode: {}, lastReadType: {}", payload, quorumPeerSubnodeId, lastReadType);
        final int type =  packet.getType();
        if (type != Leader.ACK) {
            LOG.debug("Follower is about to reply a message to leader which is not an ACK. (type={})", type);
            proceed(packet, flush);
            return;
        }

        try {
            int lastReadMessageType = lastReadType;
            if (lastReadType == Leader.UPTODATE) {
                // FollowerProcessUPTODATE
                if (quorumPeerAspect.isSyncFinished()) {
                    // for different fix code version
                    lastReadMessageType = MessageType.PROPOSAL_IN_SYNC;
                    LOG.debug("-------!!!!-------reply ACK to PROPOSAL in SYNC");
                } else {
                    LOG.debug("-------receiving UPTODATE!!!!-------begin to serve clients");
                }
            } else if (lastReadType == Leader.NEWLEADER) {
                // processing Leader.NEWLEADER
                quorumPeerAspect.setSyncFinished(false);
                if (quorumPeerAspect.isNewLeaderDone()) {
                    // for different fix code version
                    lastReadMessageType = MessageType.PROPOSAL_IN_SYNC;
                    LOG.debug("-------!!!!-------reply ACK to PROPOSAL in SYNC");
                } else {
                    LOG.debug("-------receiving NEWLEADER!!!!-------reply ACK");
                }
                quorumPeerAspect.setNewLeaderDone(true);
            } else {
                LOG.debug("This event will not be intercepted when lastReadType = {}", lastReadType);
                proceed(packet, flush);
                return;
            }

            quorumPeerAspect.setSubnodeSending();
            final long zxid = packet.getZxid();
            final int followerWritePacketId = quorumPeerAspect.getTestingService()
                    .offerFollowerToLeaderMessage(quorumPeerSubnodeId, zxid, payload, lastReadMessageType);

            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(quorumPeerSubnodeId, followerWritePacketId);

            if (lastReadType == Leader.UPTODATE) {
                quorumPeerAspect.setSyncFinished(true);
                quorumPeerAspect.getTestingService().readyForBroadcast(quorumPeerSubnodeId);
            }

            // Trick: set RECEIVING state here
            quorumPeerAspect.getTestingService().setReceivingState(quorumPeerSubnodeId);

            // to check if the partition happens
            if (followerWritePacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
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
