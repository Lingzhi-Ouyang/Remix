package org.disalg.met.server.statistics;

public interface Statistics {

    void startTimer();

    void endTimer();

    void reportTotalExecutedEvents(int totalExecutedEvents);

    void reportResult(String result);

    void reportCurrentStep(String currentStepEvent);

    void reportRandomSeed(long seed);

}
