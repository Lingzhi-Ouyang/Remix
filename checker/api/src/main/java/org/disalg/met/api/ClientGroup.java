package org.disalg.met.api;

import org.disalg.met.api.configuration.SchedulerConfigurationException;

public interface ClientGroup {
    void startClient(int client);

    void stopClient(int client);

    void configureClients(int executionId) throws SchedulerConfigurationException;

    void startClients();

    void stopClients();
}
