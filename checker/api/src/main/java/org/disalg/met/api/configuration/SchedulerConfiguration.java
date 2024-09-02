package org.disalg.met.api.configuration;

import java.io.File;

public interface SchedulerConfiguration {

    void load(String[] args) throws SchedulerConfigurationException;

    void registerPostLoadListener(SchedulerConfigurationPostLoadListener listener);

    void notifyPostLoadListeners() throws SchedulerConfigurationException;

    int getNumNodes();

    void setNumNodes(int num);

    int getNumCrashes();

    int getNumCrashesAfterElection();

    int getNumReboots();

    int getNumRebootsAfterElection();

    int getNumClients();

    int getNumReaders();

    int getNumWriters();

    int getNumClientRequests();

    int getMaxEvents();

    int getNumPriorityChangePoints();

    boolean hasRandomSeed();

    long getRandomSeed();

    long getTickTime();

    int getNumExecutions();

    String getExecutionFile();

    String getStatisticsFile();

    String getBugReportFile();

    String getMatchReportFile();

    String getSchedulingStrategy();

    void configureNode(String executionId, int nodeId, String tag) throws SchedulerConfigurationException;

    int getClientPort();

    File getWorkingDir();

    File getTraceDir();

}
