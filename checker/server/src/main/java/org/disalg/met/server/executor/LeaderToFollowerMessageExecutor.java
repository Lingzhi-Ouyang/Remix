package org.disalg.met.server.executor;

import org.disalg.met.api.*;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.LeaderToFollowerMessageEvent;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class LeaderToFollowerMessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderToFollowerMessageExecutor.class);

    private final TestingService testingService;

    public LeaderToFollowerMessageExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final LeaderToFollowerMessageEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed learner handler message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing leader message: {}", event.toString());
        releaseLeaderToFollowerMessage(event);
        testingService.getControlMonitor().notifyAll();
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Learner handler message executed: {}\n\n\n", event.toString());
        return true;
    }

    /***
     * From leader to follower
     * set sendingSubnode and receivingSubnode SYNC_PROCESSOR / COMMIT_PROCESSOR to PROCESSING
     */
    public void releaseLeaderToFollowerMessage(final LeaderToFollowerMessageEvent event) {
        testingService.setMessageInFlight(event.getId());
        final int sendingSubnodeId = event.getSendingSubnodeId();
        final Subnode sendingSubnode = testingService.getSubnodes().get(sendingSubnodeId);

        // set the sending subnode to be PROCESSING
        sendingSubnode.setState(SubnodeState.PROCESSING);

        if (event.getFlag() == TestingDef.RetCode.EXIT) {
            return;
        }

        // if in partition, then just drop it
        final int leaderId = sendingSubnode.getNodeId();
        final int followerId = event.getReceivingNodeId();
        LOG.debug("partition map: {}, leader: {}, follower: {}", testingService.getPartitionMap(), leaderId, followerId);
        if (testingService.getPartitionMap().get(leaderId).get(followerId) ||
            event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION) {
            return;
        }

        // not in partition, so the message can be received
        // set the receiving subnode to be PROCESSING
        final int type = event.getType();
        final NodeState followerState = testingService.getNodeStates().get(followerId);

        // release the leader's message,
        // and then wait for the target follower to be at the next intercepted point
        if (NodeState.ONLINE.equals(followerState)) {
            switch (type) {
                case MessageType.LEADERINFO:   // releasing my LEADERINFO
                    LOG.info("leader releases LEADERINFO and follower will reply ACKEPOCH: {}", event);
                    // Post-condition: FollowerSendACKEPOCH by the follower's QUORUM_PEER
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeTypeSending(followerId, SubnodeType.QUORUM_PEER);
                    break;
                case MessageType.DIFF:    // releasing my DIFF
                case MessageType.TRUNC: // releasing my TRUNC
                case MessageType.SNAP: // releasing my SNAP
                    LOG.info("Leader sends DIFF / TRUNC / SNAP that follower will not reply : {}", event);

                    if (type == MessageType.TRUNC) {
                        long lastZxid = event.getZxid();
                        List<Long> zxidRecord = testingService.getAllZxidRecords().get(followerId);
                        final int idx = zxidRecord.indexOf(lastZxid);
                        if (idx >= 0) {
                            // truncate to lastZxid
//                            int len = zxidRecord.size();
//                            for (int i = len - 1; i > idx; i--) {
//                                zxidRecord.remove(i);
//                            }
                            testingService.getAllZxidRecords().set(followerId, zxidRecord.subList(0,idx + 1));
                            LOG.info("After receiving leader {}'s TRUNC message, " +
                                    "follower {}'s history might change: {}", leaderId, followerId, testingService.getAllZxidRecords());
                        }
                    } else if (type == MessageType.SNAP) {
                        long lastZxid = event.getZxid();
                        List<Long> zxidRecord = testingService.getAllZxidRecords().get(leaderId);
                        final int idx = zxidRecord.indexOf(lastZxid);
                        // Using leader's history as the standard
                        testingService.getAllZxidRecords().set(followerId, zxidRecord.subList(0,idx + 1));
                        LOG.info("After receiving leader {}'s SNAP message, " +
                                "follower {}'s history might change: {}", leaderId, followerId, testingService.getAllZxidRecords());
                    }

                        // Post-condition: Leader will send NEWLEADER at last anyway by the leader's LearnerHandlerSender
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeInSendingState(sendingSubnodeId);

                    // Post-condition: this is for zk-3.4 where LearnerHandlerSender might be created here
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitFollowerMappingLearnerHandlerSender(followerId);

                    // Post-condition: LearnerHandlerReadRecord by the leader's LearnerHandler
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerMap(followerId));
                    break;
                case MessageType.NEWLEADER: // releasing my NEWLEADER
                    // --------------UPDATE 22/12-------------
                    // Updated 22/12: add an interceptor point after updating currentEpoch file
                    // Post-condition: SubmitLoggingTaskInProcessingNEWLEADER by the follower's QUORUM_PEER
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeTypeSending(followerId, SubnodeType.QUORUM_PEER);

//                    // let leader's corresponding learnerHandler be intercepted at ReadRecord
//                    // Note: sendingSubnodeId is the learnerHandlerSender, not learnerHandler
//                    testingService.getControlMonitor().notifyAll();
//                    testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerMap(followerId));

                    break;
                case MessageType.UPTODATE: // releasing my UPTODATE
                    LOG.info("leader releases UPTODATE and follower will reply ACK: {}", event);
                    // Post-condition: FollowerSendACKtoUPTODATE by the follower's QUORUM_PEER
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeTypeSending(followerId, SubnodeType.QUORUM_PEER);

                    // let leader's corresponding learnerHandler be intercepted at ReadRecord
                    // Post-condition: LearnerHandlerReadRecord by the leader's LearnerHandler
                    // ATTENTION! sendingSubnodeId is the learnerHandlerSender, not learnerHandler
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerMap(followerId));

                    // let leader's QUORUM_PEER be intercepted at LeaderJudgeIsRunning
                    // Post-condition: LeaderJudgeIsRunning by the leader's QUORUM_PEER
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitSubnodeTypeSending(leaderId, SubnodeType.QUORUM_PEER);
                    break;
                case MessageType.PROPOSAL:  // releasing my PROPOSAL
                    // for leader's PROPOSAL in sync, follower will not produce any intercepted event
                    // follower just add the proposal into the packetsNotCommitted queue
                    // in zk-3.5/6/7/8: follower will call logRequest(..) when processing NEWLEADER
                    if (Phase.BROADCAST.equals(testingService.getNodePhases().get(followerId))) {
                        // Post-condition: FollowerLogPROPOSAL by the follower's SYNC_PROCESSOR
                        testingService.getControlMonitor().notifyAll();
                        testingService.waitSubnodeTypeSending(followerId, SubnodeType.SYNC_PROCESSOR);
                    }
                    break;
                case MessageType.COMMIT:  // releasing my COMMIT
                    // for leader's COMMIT in sync, follower will not produce any intercepted event
                    // follower just add the proposal into the packetsNotCommitted queue
                    if (Phase.BROADCAST.equals(testingService.getNodePhases().get(followerId))) {
                        // Post-condition: FollowerCommit by the follower's SYNC_PROCESSOR
                        testingService.getControlMonitor().notifyAll();
                        testingService.waitSubnodeTypeSending(followerId, SubnodeType.COMMIT_PROCESSOR);
                    }
                    break;
                case TestingDef.MessageType.learnerHandlerReadRecord: // releasing my learnerHandlerReadRecord
//                    if (Phase.SYNC.equals(testingService.getNodePhases().get(followerId))) {
//                        // must be going to send UPTODATE
//                        testingService.getControlMonitor().notifyAll();
//                        testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerSenderMap(followerId));
//                    }
//                    LOG.info("release learner handler to read a message : {}", event);
//                    testingService.getControlMonitor().notifyAll();
//                    testingService.waitSubnodeInSendingState(sendingSubnodeId);
                    break;
                default:
                    LOG.info("leader sends a message : {}", event);
                    break;
            }
        }
    }
}
