package org.disalg.met.server.statistics;

public class POSstatistics implements Statistics {

    private int sumEnabledEvents;
    private int maxEnabledEvents;
    private int countEnabledEvents;

    private String currentStepEvent = null;

    @Override
    public void reportCurrentStep(String curerntStepEvent) {
        this.currentStepEvent = curerntStepEvent;
    }

    private long startTime;

    @Override
    public void startTimer() {
        startTime = System.currentTimeMillis();
    }

    private long endTime;

    @Override
    public void endTimer() {
        endTime = System.currentTimeMillis();
    }

    private String result;

    @Override
    public void reportResult(final String result) {
        this.result = result;
    }

    private int totalExecutedEvents;

    @Override
    public void reportTotalExecutedEvents(final int totalExecutedEvents) {
        this.totalExecutedEvents = totalExecutedEvents;
    }

    private long seed;

    @Override
    public void reportRandomSeed(final long seed) {
        this.seed = seed;
    }

    @Override
    public String toString() {
        final long totalTime = endTime - startTime;
        return "POSstatistics{" +
                "\n  randomSeed = " + seed +
                "\n, totalEvents = " + totalExecutedEvents +
                "\n, totalTime = " + totalTime + " ms" +
                "\n, result = " + result +
                "\n}";
    }

}
