package org.disalg.met.server.statistics;

import java.util.HashMap;
import java.util.Map;

public class POSdStatistics implements Statistics {


    private String priorityChangePointsString;
    private int totalNumRacy;

    private long startTime;

    private String currentStepEvent = null;

    @Override
    public void reportCurrentStep(String curerntStepEvent) {
        this.currentStepEvent = curerntStepEvent;
    }

    public void reportNumRacy(int numRacy)
    {
        totalNumRacy = numRacy;
    }

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

    private int numPriorityChangePoints;

    public void reportNumPriorityChangePoints(final int numPriorityChangePoints) {
        this.numPriorityChangePoints = numPriorityChangePoints;
    }

    public void reportPriorityChangePoints(final Map<Integer, Integer> priorityChangePoints) {
        final Map<Integer, Integer> reverse = new HashMap<>();
        for (final Map.Entry<Integer, Integer> entry : priorityChangePoints.entrySet()) {
            reverse.put(entry.getValue(), entry.getKey());
        }
        final StringBuilder sb = new StringBuilder("[");
        String sep = "";
        for (final Map.Entry<Integer, Integer> entry : reverse.entrySet()) {
            sb.append(sep);
            sb.append(entry.getValue());
            sep = ", ";
        }
        sb.append("]");
        priorityChangePointsString = sb.toString();
    }

    @Override
    public String toString() {
        final long totalTime = endTime - startTime;
        return "POSdStatistics{" +
                "\n  randomSeed = " + seed +
                "\n, totalEvents = " + totalExecutedEvents +
                "\n, totalNumRacy = "  + totalNumRacy +
                "\n, numPriorityChangePoints = " + numPriorityChangePoints +
                "\n, priorityChangePoints = " + priorityChangePointsString +
                "\n, totalTime = " + totalTime + " ms" +
                "\n, result = " + result +
                "\n}";
    }

}
