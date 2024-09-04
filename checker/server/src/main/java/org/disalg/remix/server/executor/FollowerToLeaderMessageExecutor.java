package org.disalg.remix.server.executor;

import org.disalg.remix.api.*;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.event.FollowerToLeaderMessageEvent;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FollowerToLeaderMessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerToLeaderMessageExecutor.class);

    private final ReplayService replayService;

    public FollowerToLeaderMessageExecutor(final ReplayService replayService) {
        this.replayService = replayService;
    }

    @Override
    public boolean execute(final FollowerToLeaderMessageEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed follower message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing message: {}", event.toString());
        releaseFollowerToLeaderMessage(event);
        replayService.getControlMonitor().notifyAll();
        replayService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Follower message event executed: {}\n\n\n", event.toString());
        return true;
    }

    /***
     * For message events from follower to leader
     * set sendingSubnode and receivingSubnode to PROCESSING
     */
    public void releaseFollowerToLeaderMessage(final FollowerToLeaderMessageEvent event) {
        replayService.setMessageInFlight(event.getId());
        final Subnode sendingSubnode = replayService.getSubnodes().get(event.getSendingSubnodeId());

        // set the sending subnode to be PROCESSING
        sendingSubnode.setState(SubnodeState.PROCESSING);

        if (event.getFlag() == TestingDef.RetCode.EXIT) {
            return;
        }

        // if in partition, then just drop it
        final int followerId = sendingSubnode.getNodeId();
        final int leaderId = event.getReceivingNodeId();
        LOG.debug("partition map: {}, follower: {}, leader: {}", replayService.getPartitionMap(), followerId, leaderId);
        if (replayService.getPartitionMap().get(followerId).get(leaderId) ||
                event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION) {
            return;
        }

        // not in partition, so the message can be received
        // set the receiving subnode to be PROCESSING
        final int lastReadType = event.getType();
        final NodeState nodeState = replayService.getNodeStates().get(leaderId);
//        Set<Subnode> subnodes = replayService.getSubnodeSets().get(leaderId);
        final Phase followerPhase = replayService.getNodePhases().get(followerId);

        if (NodeState.ONLINE.equals(nodeState)) {
            switch (lastReadType) {
                case MessageType.LEADERINFO:   // releasing my ACKEPOCH
                    replayService.getNodePhases().set(leaderId, Phase.SYNC);
                    replayService.getNodePhases().set(followerId, Phase.SYNC);

                    LOG.info("follower replies ACKEPOCH : {}", event);
                    long leaderAcceptedEpoch = replayService.getAcceptedEpoch(leaderId);
                    // Post-condition: wait for leader update currentEpoch file
                    replayService.getControlMonitor().notifyAll();
                    replayService.waitCurrentEpochUpdated(leaderId, leaderAcceptedEpoch);
                    break;
                case MessageType.NEWLEADER:     // releasing my ACK-LD.
                    // ---------------DEPRECATED---------------
                    // This is DEPRECATED since a new interceptor is added at setCurrentEpochInProcessingNEWLEADER
                    // ---------------DEPRECATED---------------

                    // Post-condition:
                    // let leader's corresponding learnerHandler process this ACK,
                    // then again be intercepted at ReadRecord
                    replayService.getControlMonitor().notifyAll();
                    replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerMap(followerId));

                    // Post-condition: LeaderSendUPTODATE by leader's corresponding learnerHandlerSender
                    replayService.getControlMonitor().notifyAll();
                    replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerSenderMap(followerId));
                    break;
                case MessageType.UPTODATE:      // releasing my ACK
                    // Post-condition: wait for follower's syncProcessorExisted && commitProcessorExisted
                    replayService.getControlMonitor().notifyAll();
                    replayService.waitFollowerSteadyAfterProcessingUPTODATE(followerId);

                    // Post-condition:
                    // let leader's corresponding learnerHandler process this ACK,
                    // then again be intercepted at ReadRecord
                    replayService.getControlMonitor().notifyAll();
                    replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerMap(followerId));
                    break;
                case MessageType.PROPOSAL:      // releasing my ACK
                    if (followerPhase.equals(Phase.BROADCAST)) {
                        LOG.info("follower replies to previous PROPOSAL message type : {}", event);

                        // Post-condition:
                        // let leader's corresponding learnerHandler process this ACK,
                        // then again be intercepted at ReadRecord
                        replayService.getControlMonitor().notifyAll();
                        replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerMap(followerId));

                        // Post-condition: let leader's corresponding learnerHandlerSender be sending COMMIT
                        replayService.getControlMonitor().notifyAll();
                        replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerSenderMap(followerId));
                    }
                    break;
                case MessageType.PROPOSAL_IN_SYNC:
                    if (followerPhase.equals(Phase.BROADCAST)) {
                        LOG.info("follower replies to previous PROPOSAL_IN_SYNC message type : {}", event);

                        // Post-condition:
                        // let leader's corresponding learnerHandler process this ACK,
                        // then again be intercepted at ReadRecord
                        replayService.getControlMonitor().notifyAll();
                        replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerMap(followerId));
                    }
                    break;
                case MessageType.COMMIT:
                    LOG.warn("SOMETHING WRONG! Actually, FollowerCommit is a local event, " +
                            "and a follower SHOULD not reply a COMMIT message: {}", event);
                    break;
                default:
                    LOG.info("follower replies to previous message type : {}", event);
            }
        }
    }

    public boolean quorumSynced(final long zxid) {
        if (replayService.getZxidSyncedMap().containsKey(zxid)){
            final int count = replayService.getZxidSyncedMap().get(zxid);
            final int nodeNum = replayService.getSchedulerConfiguration().getNumNodes();
            return count > nodeNum / 2;
        }
        return false;
    }
}
