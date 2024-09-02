package org.disalg.met.api;

import org.disalg.met.api.configuration.SchedulerConfigurationException;

public interface Ensemble {

    void startNode(int node);

    void stopNode(int node);

    void configureEnsemble(String executionId) throws SchedulerConfigurationException;

    void configureEnsemble(String executionId, int serverNum) throws SchedulerConfigurationException;

    void startEnsemble();

    void stopEnsemble();
}
