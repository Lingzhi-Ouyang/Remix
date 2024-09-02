package org.disalg.met.server.predicate;

import org.disalg.met.api.SubnodeState;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FollowerSteadyAfterProcessingUPTODATE implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyBeforeRequest.class);

    private final TestingService testingService;

    private final int followerId;

    public FollowerSteadyAfterProcessingUPTODATE(final TestingService testingService, final int followerId) {
        this.testingService = testingService;
        this.followerId = followerId;
    }

    @Override
    public boolean isTrue() {
        boolean syncProcessorExisted = false;
        boolean commitProcessorExisted = false;
//        boolean followerProcessorExisted = false;
        for (final Subnode subnode : testingService.getSubnodeSets().get(followerId)) {
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
