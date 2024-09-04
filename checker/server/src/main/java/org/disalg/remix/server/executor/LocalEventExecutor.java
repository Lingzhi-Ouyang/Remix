package org.disalg.remix.server.executor;

import org.disalg.remix.api.*;
import org.disalg.remix.api.state.LeaderElectionState;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.event.LocalEvent;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/***
 * Executor of local event
 */
public class LocalEventExecutor extends BaseEventExecutor{
    private static final Logger LOG = LoggerFactory.getLogger(LocalEventExecutor.class);

    private final ReplayService replayService;

    public LocalEventExecutor(final ReplayService replayService) {
        this.replayService = replayService;
    }

    @Override
    public boolean execute(final LocalEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed local event: {}", event.toString());
            return false;
        }
        LOG.debug("Processing request: {}", event.toString());
        releaseLocalEvent(event);
        replayService.getControlMonitor().notifyAll();
        replayService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Local event executed: {}\n\n\n", event.toString());
        return true;
    }

    // Should be called while holding a lock on controlMonitor
    /***
     * For sync
     * set SYNC_PROCESSOR / COMMIT_PROCESSOR to PROCESSING
     * @param event
     */
    public void releaseLocalEvent(final LocalEvent event) {
        replayService.setMessageInFlight(event.getId());
        SubnodeType subnodeType = event.getSubnodeType();

        final int subnodeId = event.getSubnodeId();
        final Subnode subnode = replayService.getSubnodes().get(subnodeId);
        final int nodeId = subnode.getNodeId();

        // set the corresponding subnode to be PROCESSING
        subnode.setState(SubnodeState.PROCESSING);

        if (event.getFlag() == TestingDef.RetCode.EXIT || event.getFlag() == TestingDef.RetCode.NO_WAIT) {
            return;
        }

        // set the next subnode to be PROCESSING
        switch (subnodeType) {
            case LEARNER_HANDLER:
                final int followerId = replayService.getFollowerSocketAddressBook().indexOf(event.getPayload());
                LOG.debug("Leader {} about to sync with follower {}.", nodeId, followerId);
                // Post-condition:
                // for zk-3.5/6/7/8:
                // - DIFF / TRUNC: let follower mapping to the leader's corresponding learnerHandlerSender
                // - SNAP: the corresponding learnerHandlerSender will not be created here
                replayService.getControlMonitor().notifyAll();
                replayService.waitSyncTypeDetermined(followerId);
                final int syncType = replayService.getSyncType(followerId);
                LOG.info("Leader {} is going to sync with follower {} using {}", nodeId, followerId, syncType);

                if ( syncType == MessageType.DIFF || syncType == MessageType.TRUNC ) {
                    // Post-condition: let follower mapping to the leader's corresponding learnerHandlerSender
                    replayService.getControlMonitor().notifyAll();
                    replayService.waitFollowerMappingLearnerHandlerSender(followerId);
                    // Post-condition for DIFF / TRUNC: let leader's corresponding learnerHandlerSender sending DIFF / TRUNC
                    replayService.getControlMonitor().notifyAll();
                    replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerSenderMap(followerId));
                } else {
                    // Post-condition for SNAP: let leader's corresponding learnerHandler sending SNAP
                    replayService.getControlMonitor().notifyAll();
                    replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerMap(followerId));
                }
                break;
            case QUORUM_PEER:
                // follower: FollowerProcessSyncMessage / FollowerProcessPROPOSALInSync
                //              / FollowerProcessCOMMITInSync / SubmitLoggingTaskInProcessingNEWLEADER
                // leader: LeaderJudgingIsRunning
                LeaderElectionState role = replayService.getLeaderElectionState(nodeId);
                if (role.equals(LeaderElectionState.FOLLOWING)) {
                    int eventType = event.getType();
                    if (eventType == TestingDef.MessageType.NEWLEADER) {
                        // post-condition:
                        // 1. follower reply ACK-LD: FollowerSendACKtoNEWLEADER by follower's QUORUM_PEER
                        // 2. follower log a proposal: LogRequest by follower's SYNC_PROCESSOR
                        LOG.debug("Follower {} has updated CurrentEpoch during processing NEWLEADER.", nodeId);
//                        replayService.getControlMonitor().notifyAll();
//                        replayService.waitSubnodeTypeSending(nodeId, SubnodeType.QUORUM_PEER);
//
//                        // let leader's corresponding learnerHandler be intercepted at ReadRecord
//                        replayService.getControlMonitor().notifyAll();
//                        replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerMap(nodeId));
                    }
                    else {
                        // TODO: deprecated since this branch will not be triggered!
                        LOG.warn("This branch is not supposed to occur!!! Local event: {}", event);
                        if (eventType == MessageType.DIFF || eventType == MessageType.TRUNC || eventType == MessageType.SNAP) {
                            replayService.getSyncTypeList().set(event.getNodeId(), event.getType());
                        }
                        // for FollowerProcessSyncMessage / FollowerProcessPROPOSALInSync / FollowerProcessCOMMITInSync:
                        //  Post-condition:  wait for the follower's QUORUM_PEER thread to SENDING state
                        //      since ACK_LD will come at last anyway
                        replayService.getControlMonitor().notifyAll();
                        replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerSenderMap(nodeId));
                        replayService.getControlMonitor().notifyAll();
                        replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerMap(nodeId));
                    }
                }
                break;
            case SYNC_PROCESSOR: // leader / follower do log
                final Long zxid = event.getZxid();
                Map<Long, Integer> zxidSyncedMap = replayService.getZxidSyncedMap();
                replayService.getZxidSyncedMap().put(zxid, zxidSyncedMap.getOrDefault(zxid, 0) + 1);

                // for log event and ack event
                if (LeaderElectionState.FOLLOWING.equals(replayService.getLeaderElectionState(nodeId))
                        && event.getFlag() != TestingDef.RetCode.NO_WAIT) {
                    if (Phase.BROADCAST.equals(replayService.getNodePhases().get(nodeId))) {
                        // During broadcast:
                        // leader will ack self, which is not intercepted
                        // follower will send ACK message to leader, which is intercepted only in follower
                        LOG.debug("Wait for follower {}'s SYNC thread to be SENDING ACK", event.getNodeId());
                        // Post-condition: FollowerSendACKtoPROPOSAL by follower's SYNC_PROCESSOR
                        replayService.getControlMonitor().notifyAll();
                        replayService.waitSubnodeInSendingState(subnodeId);
                    } else {
                        // During sync:
                        // post-condition:
                        // 1. follower reply ACK-LD: FollowerSendACKtoNEWLEADER by follower's QUORUM_PEER
                        // 2. follower log a proposal: LogRequest by follower's SYNC_PROCESSOR
                        LOG.debug("Follower {} has logged a proposal during sync.", nodeId);
                        replayService.getControlMonitor().notifyAll();
                    }

                }
                break;
            case COMMIT_PROCESSOR:  // leader / follower do commit
                // Post-condition: wait for the node update its lastProcessedZxid
                replayService.getControlMonitor().notifyAll();
                replayService.waitCommitProcessorDone(event.getId(), event.getNodeId());
                break;
        }
    }
}
