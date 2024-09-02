package org.disalg.met.server.executor;

import org.disalg.met.api.Phase;
import org.disalg.met.api.state.LeaderElectionState;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.PartitionStartEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

public class PartitionStartExecutor extends BaseEventExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionStopExecutor.class);
    private final TestingService testingService;

    //TODO: + partitionBudget configuration
    private int partitionBudget;

    public PartitionStartExecutor(final TestingService testingService, final int partitionBudget) {
        this.testingService = testingService;
        this.partitionBudget = partitionBudget;
    }

    @Override
    public boolean execute(final PartitionStartEvent event) throws IOException {
        boolean truelyExecuted = false;
        if (enablePartition()) {
            startPartition(event.getNode1(), event.getNode2());
            testingService.getControlMonitor().notifyAll();
            testingService.waitAllNodesSteady();
            partitionBudget--;
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean enablePartition() {
        return partitionBudget > 0;
    }

    /***
     * Called by the partition start executor
     * partition between a leader and a follower will make the follower back into LOOKING state
     * then, if the leader loses quorum, the leader will be back into LOOKING too (similar to the effects of stopNode)
     * @return
     */
    public void startPartition(final int node1, final int node2) {
        // 1. PRE_EXECUTION: set unstable state

        // 2. EXECUTION
        List<List<Boolean>> partitionMap = testingService.getPartitionMap();
        LOG.debug("start partition: {} & {}", node1, node2);
        LOG.debug("before partition: {}, {}, {}", partitionMap.get(0), partitionMap.get(1), partitionMap.get(2));
        partitionMap.get(node1).set(node2, true);
        partitionMap.get(node2).set(node1, true);
        LOG.debug("after partition: {}, {}, {}", partitionMap.get(0), partitionMap.get(1), partitionMap.get(2));


        // 3. POST_EXECUTION: wait for the state to be stable (set ONLINE)

        // if leader & follower, wait for leader / follower into LOOKING
        List<LeaderElectionState> leaderElectionStates = testingService.getLeaderElectionStates();
        LeaderElectionState role1 = leaderElectionStates.get(node1);
        LeaderElectionState role2 = leaderElectionStates.get(node2);
        LOG.debug("Node {} & {} partition start.\n\n\n ", node1, node2);

//        // release all nodes' event related to the partitioned nodes
//        testingService.getControlMonitor().notifyAll();

        // leader & follower: need to set related nodes back to LOOKING state and release broadcast events
        boolean leaderExist = LeaderElectionState.LEADING.equals(role1) || LeaderElectionState.LEADING.equals(role2);
        boolean followerExist = LeaderElectionState.FOLLOWING.equals(role1) || LeaderElectionState.FOLLOWING.equals(role2);
        if (leaderExist && followerExist) {
            int leader = LeaderElectionState.LEADING.equals(role1) ? node1 : node2;
            int follower = LeaderElectionState.FOLLOWING.equals(role1) ? node1 : node2;
            LOG.debug("Leader {} & Follower {} get partition.", leader, follower);

            // if quorum breaks, wait for the leader into LOOKING
            int nodeNum = testingService.getSchedulerConfiguration().getNumNodes();
            testingService.getParticipants().remove(follower);
            int participantCount = testingService.getParticipants().size();
            if (participantCount <= (nodeNum / 2)) {
                testingService.getParticipants().clear();
                // leader & follower need to change node state to LOOKING
                LOG.debug("Leader's quorum peers count {} less than half the node num {}!  " +
                        "Wait for leader {} to be LOOKING", participantCount, nodeNum, leader);
                // Predicate AliveNodesInLookingState will releaseBroadcastEvent
                LOG.debug("Try to set flag NODE_PAIR_IN_PARTITION to relative events before the node get into LOOKING...");
                testingService.recordPartitionedEvent(new HashSet<Integer>() {{
                    add(node1);
                    add(node2);
                }}, true);
                testingService.getControlMonitor().notifyAll();
                testingService.waitAliveNodesInLookingState(new HashSet<Integer>() {{
                    add(leader);
                    add(follower);
                }});
            } else {
                // leader : no need to change node state
                // follower: need to change node state
                LOG.debug("wait for follower {} back into LOOKING.", follower);
                LOG.debug("Try to set flag NODE_PAIR_IN_PARTITION to relative events before the node get into LOOKING...");
                testingService.recordPartitionedEvent(new HashSet<Integer>() {{
                    add(node1);
                    add(node2);
                }}, false);
                testingService.getControlMonitor().notifyAll();
                testingService.waitAliveNodesInLookingState(new HashSet<Integer>() {{
                    add(follower);
                }});
            }
        } else {
            // leader & candidate / follower & candidate: no need to change node state
            LOG.debug("Try to set flag NODE_PAIR_IN_PARTITION to relative events before the node get into LOOKING...");
            testingService.recordPartitionedEvent(new HashSet<Integer>() {{
                add(node1);
                add(node2);
            }}, false);
        }
    }
}
