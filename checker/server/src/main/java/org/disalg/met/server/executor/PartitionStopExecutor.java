package org.disalg.met.server.executor;

import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.PartitionStopEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PartitionStopExecutor extends BaseEventExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionStopExecutor.class);
    private final TestingService testingService;

    //TODO: + partitionBudget
    private int partitionStopBudget;

    public PartitionStopExecutor(final TestingService testingService, final int partitionStopBudget) {
        this.testingService = testingService;
        this.partitionStopBudget = partitionStopBudget;
    }

    @Override
    public boolean execute(final PartitionStopEvent event) throws IOException {
        boolean truelyExecuted = false;
        if (enablePartitionStop()) {
            stopPartition(event.getNode1(), event.getNode2());
            testingService.getControlMonitor().notifyAll();
            testingService.waitAllNodesSteady();
            partitionStopBudget--;
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean enablePartitionStop() {
        return partitionStopBudget > 0;
    }

    /***
     * Called by the partition stop executor
     * @return
     */
    public void stopPartition(final int node1, final int node2) {
        // 1. PRE_EXECUTION: set unstable state (set STARTING)
//        nodeStates.set(node1, NodeState.STARTING);
//        nodeStates.set(node2, NodeState.STARTING);

        List<List<Boolean>> partitionMap = testingService.getPartitionMap();
        // 2. EXECUTION
        partitionMap.get(node1).set(node2, false);
        partitionMap.get(node2).set(node1, false);

        // wait for the state to be stable (set ONLINE)
//        nodeStates.set(node1, NodeState.ONLINE);
//        nodeStates.set(node2, NodeState.ONLINE);

        testingService.getControlMonitor().notifyAll();
    }
}
