package org.disalg.remix.server.statistics;

public interface Statistics {

    void startTimer();

    void endTimer();

    void reportTotalExecutedEvents(int totalExecutedEvents);

    void reportResult(String result);

    void reportCurrentStep(String currentStepEvent);

    void reportRandomSeed(long seed);

}
