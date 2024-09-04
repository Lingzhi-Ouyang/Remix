package org.disalg.remix.server.predicate;

import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerSteadyAfterProcessingUPTODATE implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyBeforeRequest.class);

    private final ReplayService replayService;

    private final int followerId;

    public FollowerSteadyAfterProcessingUPTODATE(final ReplayService replayService, final int followerId) {
        this.replayService = replayService;
        this.followerId = followerId;
    }

    @Override
    public boolean isTrue() {
        boolean syncProcessorExisted = false;
        boolean commitProcessorExisted = false;
//        boolean followerProcessorExisted = false;
        for (final Subnode subnode : replayService.getSubnodeSets().get(followerId)) {
            switch (subnode.getSubnodeType()) {
                case SYNC_PROCESSOR:
                    syncProcessorExisted = true;
                    break;
                case COMMIT_PROCESSOR:
                    commitProcessorExisted = true;
                    break;
//                case FOLLOWER_PROCESSOR:
//                    followerProcessorExisted = true;
//                    break;
                default:
            }
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                LOG.debug("------Not steady for follower's {} thread-----" +
                                "Node {} subnode {} status: {}\n",
                        subnode.getSubnodeType(), followerId, subnode.getId(), subnode.getState());
                return false;
            }
            LOG.debug("-----------Follower node {} subnode {} status: {}, subnode type: {}",
                    followerId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
        }
//        return syncProcessorExisted && commitProcessorExisted && followerProcessorExisted;
        return syncProcessorExisted && commitProcessorExisted ;
    }

    @Override
    public String describe() {
        return "follower " + followerId +  " steady before request";
    }


}
