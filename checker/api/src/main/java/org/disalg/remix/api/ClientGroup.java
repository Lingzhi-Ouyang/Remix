package org.disalg.remix.api;

import org.disalg.remix.api.configuration.SchedulerConfigurationException;

public interface ClientGroup {
    void startClient(int client);

    void stopClient(int client);

    void configureClients(int executionId) throws SchedulerConfigurationException;

    void startClients();

    void stopClients();
}
