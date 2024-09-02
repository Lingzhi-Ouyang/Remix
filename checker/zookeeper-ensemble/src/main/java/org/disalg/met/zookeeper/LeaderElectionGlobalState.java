package org.disalg.met.zookeeper;

import org.disalg.met.api.configuration.SchedulerConfigurationException;
import org.disalg.met.api.configuration.SchedulerConfigurationPostLoadListener;
import org.disalg.met.api.state.GlobalState;
import org.disalg.met.api.state.GlobalStateFinalListener;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;

public class LeaderElectionGlobalState implements GlobalState, SchedulerConfigurationPostLoadListener {

    public enum Role {
        LOOKING, FOLLOWING, LEADING
    }

    @Autowired
    private ZookeeperConfiguration zookeeperConfiguration;

    private List<Role> roles;
    private List<Boolean> online;

    @Override
    public void postLoadCallback() throws SchedulerConfigurationException {
        final Role[] roles = new Role[zookeeperConfiguration.getNumNodes()];
        final Boolean[] online = new Boolean[zookeeperConfiguration.getNumNodes()];
        Arrays.fill(roles, Role.LOOKING);
        Arrays.fill(online, Boolean.FALSE);
        this.roles = Arrays.asList(roles);
        this.online = Arrays.asList(online);
    }

    @Override
    public boolean isOnline(final long node) {
        return online.get((int) node);
    }

    @Override
    public void setOnline(final long node, final boolean online) {
        this.online.set((int) node, online);
    }

    @Override
    public boolean isFinal() {
        return false;
    }

    @Override
    public void registerGobalStateFinalListener(final GlobalStateFinalListener listener) {

    }

    @Override
    public void notifyGlobalStateFinalListeners() {

    }

}
