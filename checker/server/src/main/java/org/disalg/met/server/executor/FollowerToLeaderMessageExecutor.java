package org.disalg.met.server.executor;

import org.disalg.met.api.*;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.FollowerToLeaderMessageEvent;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class FollowerToLeaderMessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerToLeaderMessageExecutor.class);

    private final TestingService testingService;

    public FollowerToLeaderMessageExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final FollowerToLeaderMessageEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed follower message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing message: {}", event.toString());
        releaseFollowerToLeaderMessage(event);
        testingService.getControlMonitor().notifyAll();
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Follower message event executed: {}\n\n\n", event.toString());
        return true;
    }

    /***
     * For message events from follower to leader
     * set sendingSubnode and receivingSubnode to PROCESSING
     */
    public void releaseFollowerToLeaderMessage(final FollowerToLeaderMessageEvent event) {
        testingService.setMessageInFlight(event.getId());
        final Subnode sendingSubnode = testingService.getSubnodes().get(event.getSendingSubnodeId());

        // set the sending subnode to be PROCESSING
        sendingSubnode.setState(SubnodeState.PROCESSING);

        if (event.getFlag() == TestingDef.RetCode.EXIT) {
            return;
        }

        // if in partition, then just drop it
        final int followerId = sendingSubnode.getNodeId();
        final int leaderId = event.getReceivingNodeId();
        LOG.debug("partition map: {}, follower: {}, leader: {}", testingService.getPartitionMap(), followerId, leaderId);
        if (testingService.getPartitionMap().get(followerId).get(leaderId) ||
                event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION) {
            return;
        }

        // not in partition, so the message can be received
        // set the receiving subnode to be PROCESSING
        final int lastReadType = event.getType();
        final NodeState nodeState = testingService.getNodeStates().get(leaderId);
//        Set<Subnode> subnodes = testingService.getSubnodeSets().get(leaderId);
        final Phase followerPhase = testingService.getNodePhases().get(followerId);

        if (NodeState.ONLINE.equals(nodeState)) {
            switch (lastReadType) {
                case MessageType.LEADERINFO:   // releasing my ACKEPOCH
                    testingService.getNodePhases().set(leaderId, Phase.SYNC);
                    testingService.getNodePhases().set(followerId, Phase.SYNC);

                    LOG.info("follower replies ACKEPOCH : {}", event);
                    long leaderAcceptedEpoch = testingService.getAcceptedEpoch(leaderId);
                    // Post-condition: wait for leader update currentEpoch file
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitCurrentEpochUpdated(leaderId, leaderAcceptedEpoch);
                    break;
                case MessageType.NEWLEADER:     // releasing my ACK-LD.
                    // ---------------DEPRECATED---------------
                    // This is DEPRECATED since a new interceptor is added at setCurrentEpochInProcessingNEWLEADER
                    // ---------------DEPRECATED---------------

                    // Post-condition:
                    // let leader's corresponding learnerHandler process this ACK,
                    // then again be intercepted at ReadRecord
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerMap(followerId));

                    // Post-condition: LeaderSendUPTODATE by leader's corresponding learnerHandlerSender
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerSenderMap(followerId));
                    break;
                case MessageType.UPTODATE:      // releasing my ACK
                    // Post-condition: wait for follower's syncProcessorExisted && commitProcessorExisted
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitFollowerSteadyAfterProcessingUPTODATE(followerId);

                    // Post-condition:
                    // let leader's corresponding learnerHandler process this ACK,
                    // then again be intercepted at ReadRecord
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerMap(followerId));
                    break;
                case MessageType.PROPOSAL:      // releasing my ACK
                    if (followerPhase.equals(Phase.BROADCAST)) {
                        LOG.info("follower replies to previous PROPOSAL message type : {}", event);

                        // Post-condition:
                        // let leader's corresponding learnerHandler process this ACK,
                        // then again be intercepted at ReadRecord
                        testingService.getControlMonitor().notifyAll();
                        testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerMap(followerId));

                        // Post-condition: let leader's corresponding learnerHandlerSender be sending COMMIT
                        testingService.getControlMonitor().notifyAll();
                        testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerSenderMap(followerId));
                    }
                    break;
                case MessageType.PROPOSAL_IN_SYNC:
                    if (followerPhase.equals(Phase.BROADCAST)) {
                        LOG.info("follower replies to previous PROPOSAL_IN_SYNC message type : {}", event);

                        // Post-condition:
                        // let leader's corresponding learnerHandler process this ACK,
                        // then again be intercepted at ReadRecord
                        testingService.getControlMonitor().notifyAll();
                        testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerMap(followerId));
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
        if (testingService.getZxidSyncedMap().containsKey(zxid)){
            final int count = testingService.getZxidSyncedMap().get(zxid);
            final int nodeNum = testingService.getSchedulerConfiguration().getNumNodes();
            return count > nodeNum / 2;
        }
        return false;
    }
}
