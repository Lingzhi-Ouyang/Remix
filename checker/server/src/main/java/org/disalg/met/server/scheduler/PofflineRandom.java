package org.disalg.met.server.scheduler;

import java.util.*;

public class PofflineRandom  {

    private final int maxEvents;
    private final int numPriorityChangePoints;
    private final Random random;

    public PofflineRandom(int maxEvents, int numPriorityChangePoints, Random random) {
        this.maxEvents = maxEvents;
        this.numPriorityChangePoints = numPriorityChangePoints;
        this.random = random;
    }

    public Map<Integer, Integer> generatePriorityChangePoints() {
        final Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < numPriorityChangePoints; i++) {
            int candidate = 1 + random.nextInt((maxEvents / 2) - i);
            for (int existing : map.keySet()) {
                if (candidate >= existing) {
                    candidate++;
                }
                else {
                    break;
                }
            }
            map.put(candidate, i);
        }
        return map;
    }

    public int generateChainPosition(int numChains) {
        assert numChains >= numPriorityChangePoints;
        return numPriorityChangePoints + random.nextInt(1 + numChains - numPriorityChangePoints);
    }
}
