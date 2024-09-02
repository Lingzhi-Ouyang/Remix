package org.disalg.met.api.configuration;

public interface SchedulerConfigurationPostLoadListener {

    void postLoadCallback() throws SchedulerConfigurationException;

}
