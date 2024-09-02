package org.disalg.met.server.scheduler;

import org.disalg.met.server.statistics.POSdStatistics;
import org.disalg.met.server.event.ElectionMessageEvent;
import org.disalg.met.server.event.Event;
import org.disalg.met.server.event.NodeCrashEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class POSdStrategy  implements SchedulingStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(POSdStrategy.class);

    private POSdStatistics statistics;
    private Map<Integer, Integer>  priorityChangePoints;
    private Map<Event, Double> priorities;
    private POSdRandom random;
    private Random rand;
    private int currNumPriority;

    public POSdStrategy(int maxEvents, int numPriorityChangePoints,Random rand, POSdStatistics statistics) {
        this.random = new POSdRandom(maxEvents, numPriorityChangePoints, rand);
        this.statistics = statistics;
        this.rand = rand;
        this.priorityChangePoints = this.random.generatePriorityChangePoints();
        priorities = new HashMap<>();
        currNumPriority = 0;
        this.statistics.reportPriorityChangePoints(priorityChangePoints);
    }

    private boolean nextEventPrepared = false;
    private Event nextEvent;

    @Override
    public void remove(Event event) {
        LOG.warn("Not implementation of removing event: {}", event.toString());
    }

    @Override
    public boolean hasNextEvent() {
        if (!nextEventPrepared) {
            prepareNextEvent();
        }
        LOG.debug("hasNextEvent == {}", nextEvent != null);
        return nextEvent != null;
    }

    @Override
    public Event nextEvent() {
        if (!nextEventPrepared) {
            prepareNextEvent();
        }
        nextEventPrepared = false;
        return nextEvent;
    }

    private void prepareNextEvent() {
        LOG.debug("Preparing next event...");

        nextEvent = getHighPriority();
        nextEventPrepared = true;
        if (nextEvent != null){
            updatePriorities();
        }
    }

    @Override
    public void add(final Event event){
        if (!priorities.keySet().contains(event)){
            double pr = rand.nextDouble();
            priorities.put(event, pr);
            LOG.debug("--- Added transition: " + event.toString() + " with ID: ‌" + event.getId() + " with priority: " + pr);
        }

        if (nextEventPrepared && nextEvent == null){
            nextEventPrepared = false;
        }
    }

    private void updatePriorities()
    {
        List<Event> keys = new ArrayList<>(priorities.keySet());
        boolean reachedPCP = true;
        while (reachedPCP){
            if (reachedPCP){
                nextEvent = getHighPriority();
                priorities.remove(nextEvent);
            }
            reachedPCP = false;
            for (Event e : keys) {
                if (e.isEnabled()) {
                    if (isRacy(nextEvent, e)) {
                        currNumPriority++;
                        statistics.reportNumRacy(currNumPriority);
                        if (priorityChangePoints.containsKey(currNumPriority)) {
                            reachedPCP = true;
                            double pr = -1 - priorityChangePoints.get(currNumPriority);
                            priorities.put(nextEvent, pr);
                            LOG.debug("--- Updated priority of " + nextEvent.getId() + " because of " + e.getId() + " with priority " + pr + " at prChangept: " + currNumPriority);
                            break;
                        }
                    }
                }
            }
        }
    }


    private Event getHighPriority() {
        double mx = Integer.MIN_VALUE;
        Event res = null;
        for (Event e : priorities.keySet()) {
            if (mx < priorities.get(e) && e.isEnabled()) {
                mx = priorities.get(e);
                res = e;
            }
        }
        return res;
    }
    private boolean isRacy (Event e1, Event e2){
        if (e1 instanceof ElectionMessageEvent && e2 instanceof ElectionMessageEvent) {
            if (((ElectionMessageEvent) e1).getReceivingNodeId() == ((ElectionMessageEvent) e2).getReceivingNodeId()) {
                return true;
            }
        }
        if (e1 instanceof ElectionMessageEvent && e2 instanceof NodeCrashEvent) {
            if (((ElectionMessageEvent) e1).getReceivingNodeId() == ((NodeCrashEvent) e2).getNodeId()) {
                return true;
            }
        }
        return false;
    }
}