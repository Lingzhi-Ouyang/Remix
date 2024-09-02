package org.disalg.met.zookeeper;

import org.disalg.met.api.state.PropertyVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class LeaderElectionVerifier implements PropertyVerifier<LeaderElectionGlobalState> {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionVerifier.class);

    @Autowired
    private ZookeeperConfiguration zookeeperConfiguration;

    @Override
    public void verify(final LeaderElectionGlobalState serverState) {

    }

}
