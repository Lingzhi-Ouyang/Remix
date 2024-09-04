package org.disalg.remix.api.configuration;

public interface SchedulerConfigurationPostLoadListener {

    void postLoadCallback() throws SchedulerConfigurationException;

}
