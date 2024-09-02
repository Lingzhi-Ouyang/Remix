package org.disalg.met.server.checker;

import org.disalg.met.server.TestingService;
import org.disalg.met.server.statistics.Statistics;
import org.disalg.met.api.NodeState;
import org.disalg.met.api.state.LeaderElectionState;
import org.disalg.met.api.state.Vote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class LeaderElectionVerifier implements Verifier {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionVerifier.class);

    private final TestingService testingService;
    private final Statistics statistics;
    private Integer modelResult;
    private Set<Integer> participants;

    public LeaderElectionVerifier(final TestingService testingService, Statistics statistics) {
        this.testingService = testingService;
        this.statistics = statistics;
        this.modelResult = null;
        this.participants = null;
    }

    public LeaderElectionVerifier(final TestingService testingService, Statistics statistics, final Set<Integer> participants) {
        this.testingService = testingService;
        this.statistics = statistics;
        this.modelResult = null;
        this.participants = participants;
    }

    public void setParticipants(Set<Integer> participants) {
        this.participants = participants;
    }

    public void setModelResult(Integer modelResult) {
        this.modelResult = modelResult;
    }

    /***
     * Verify whether the result of the leader election achieves consensus
     * @return whether the result of the leader election achieves consensus
     */
    @Override
    public boolean verify() {
        // There should be a unique leader; everyone else should be following or observing that leader
        int leader = -1;
        boolean consensus = true;
        String matchModel = "UNMATCHED";
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            if (!participants.contains(nodeId)){
                continue;
            }
            LOG.debug("--------------->Node Id: {}, NodeState: {}, " +
                            "leader: {},  isLeading: {}, " +
                            "isObservingOrFollowing:{}, {}, " +
                            "vote: {}",nodeId, testingService.getNodeStates().get(nodeId), leader,
                    isLeading(nodeId), isObservingOrFollowing(nodeId),
                    isObservingOrFollowing(nodeId, leader), testingService.getVotes().get(nodeId)
            );
            if (NodeState.OFFLINE.equals(testingService.getNodeStates().get(nodeId))) {
                continue;
            }

            /**
             * There are four acceptable cases:
             *   1. leader == -1 && isLeading(nodeId) -- Fine, nodeId is the leader
             *   2. leader == -1 && isObservingOrFollowing(nodeId) -- Fine, whoever is in the final vote is the leader
             *   3. leader == nodeId && isLeading(nodeId) -- Fine, nodeId is the leader
             *   4. leader != -1 && isObservingOrFollowing(nodeId, leader) -- Fine, following the correct leader
             * In all other cases node is either still looking, or is another leader, or is following the wrong leader
             */
            if (leader == -1 && isLeading(nodeId)) {
                leader = nodeId;
            }
            else if (leader == -1 && isObservingOrFollowing(nodeId)) {
                final Vote vote = testingService.getVotes().get(nodeId);
                if (vote == null) {
                    consensus = false;
                    break;
                }
                leader = (int) vote.getLeader();
            }
            else if (!((leader == nodeId && isLeading(nodeId)) ||
                    (leader != -1 && isObservingOrFollowing(nodeId, leader)))) {
                consensus = false;
                break;
            }
        }
        if (this.modelResult == null) {
            matchModel = "UNKNOWN";
        } else if (this.modelResult.equals(leader)){
            matchModel = "MATCHED";
        }
        if (matchModel.equals("UNMATCHED")) {
            testingService.traceMatched = false;
        }
        if (leader != -1 && consensus) {
            statistics.reportResult("ELECTION:SUCCESS:" + matchModel);
            return true;
        }
        else {
            statistics.reportResult("ELECTION:FAILURE:" + matchModel);
            testingService.tracePassed = false;
            return false;
        }
    }

    private boolean isLeading(final int nodeId) {
        final LeaderElectionState state = testingService.getLeaderElectionStates().get(nodeId);
        final Vote vote = testingService.getVotes().get(nodeId);
        // Node's state is LEADING and it has itself as the leader in the final vote
        return LeaderElectionState.LEADING.equals(state)
                && vote != null && nodeId == (int) vote.getLeader();
    }

    private boolean isObservingOrFollowing(final int nodeId, final int leader) {
        final Vote vote = testingService.getVotes().get(nodeId);
        // Node's state is FOLLOWING or OBSERVING and it has leader as the leader in the final vote
        return isObservingOrFollowing(nodeId) && vote != null && leader == (int) vote.getLeader();
    }

    private boolean isObservingOrFollowing(final int nodeId) {
        final LeaderElectionState state = testingService.getLeaderElectionStates().get(nodeId);
        return (LeaderElectionState.FOLLOWING.equals(state) || LeaderElectionState.OBSERVING.equals(state));
    }

    private boolean isLooking(final int nodeId) {
        return LeaderElectionState.LOOKING.equals(testingService.getLeaderElectionStates().get(nodeId));
    }
}
