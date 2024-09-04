package org.disalg.remix.server.checker;

import org.disalg.remix.api.NodeState;
import org.disalg.remix.api.Phase;
import org.disalg.remix.api.state.LeaderElectionState;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CommittedLogVerifier implements Verifier{

    private static final Logger LOG = LoggerFactory.getLogger(CommittedLogVerifier.class);

    private final ReplayService replayService;
    private final Statistics statistics;

    public CommittedLogVerifier(final ReplayService replayService, Statistics statistics) {
        this.replayService = replayService;
        this.statistics = statistics;
    }

    @Override
    public boolean verify() {
        boolean leaderExist = false;
        boolean leaderCommittedLogPassed = true;
        for (int nodeId = 0; nodeId < replayService.getSchedulerConfiguration().getNumNodes(); nodeId++) {
            LeaderElectionState leaderElectionState = replayService.getLeaderElectionStates().get(nodeId);
            if (! LeaderElectionState.LEADING.equals(leaderElectionState)) continue;
            leaderExist = true;
            NodeState nodeState = replayService.getNodeStates().get(nodeId);
            Phase phase = replayService.getNodePhases().get(nodeId);
            if (NodeState.ONLINE.equals(nodeState)
                    && Phase.BROADCAST.equals(phase)) {
                leaderCommittedLogPassed = checkLeaderCommittedHistory(nodeId);
            }
        }

        if (leaderExist && leaderCommittedLogPassed) {
            statistics.reportResult("LEADER_COMMITTED_LOG:SUCCESS:MATCHED");
            return true;
        }
        else if (leaderExist) {
            statistics.reportResult("LEADER_COMMITTED_LOG:FAILURE:MATCHED");
            replayService.tracePassed = false;
            return false;
        } else {
            statistics.reportResult("LEADER_COMMITTED_LOG:LEADER_NOT_EXIST:MATCHED");
            return true;
        }
    }

    /***
     * we pass SNAP for now
     * for now we just check length
     * actually we should check each item to be equal
     * @param nodeId
     * @return
     */
    private boolean checkLeaderCommittedHistory(final int nodeId) {
        List<Long> lastCommittedZxidList = replayService.getLastCommittedZxid();
        int committedLen = lastCommittedZxidList.size();
        List<Long> leaderZxidRecords = replayService.getAllZxidRecords().get(nodeId);
        int leaderRecordLen = leaderZxidRecords.size();
        if (committedLen > leaderRecordLen) return false;
        for (int i = 0; i < committedLen; i++) {
            if (!leaderZxidRecords.get(i).equals(lastCommittedZxidList.get(i)))
                return false;
        }
        if (leaderRecordLen > committedLen) {
            lastCommittedZxidList.addAll(leaderZxidRecords.subList(committedLen, leaderRecordLen));
            LOG.info("\n---Update lastCommittedZxid " + replayService.getLastCommittedZxid());
        }
        return true;
    }
}
