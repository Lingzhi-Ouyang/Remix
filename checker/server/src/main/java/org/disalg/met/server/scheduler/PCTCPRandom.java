package org.disalg.met.server.scheduler;

import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

public class PCTCPRandom {

    private final int maxEvents;
    private final int numPriorityChangePoints;
    private final Random random;

    public PCTCPRandom(int maxEvents, int numPriorityChangePoints, Random random) {
        this.maxEvents = maxEvents;
        this.numPriorityChangePoints = numPriorityChangePoints;
        this.random = random;
    }

    public Map<Integer, Integer> generatePriorityChangePoints() {
        final SortedMap<Integer, Integer> map = new TreeMap<>();
        for (int i = 0; i < numPriorityChangePoints; i++) {
            int candidate = 1 + random.nextInt(maxEvents - i);
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
