package org.disalg.remix.api;

import org.disalg.remix.api.configuration.SchedulerConfigurationException;

public interface Ensemble {

    void startNode(int node);

    void stopNode(int node);

    void configureEnsemble(String executionId) throws SchedulerConfigurationException;

    void configureEnsemble(String executionId, int serverNum) throws SchedulerConfigurationException;

    void startEnsemble();

    void stopEnsemble();
}
