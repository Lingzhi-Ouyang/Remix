package org.disalg.met.server.scheduler;

import org.disalg.met.server.statistics.RAPOSstatistics;
import org.disalg.met.server.event.Event;
import org.disalg.met.server.event.ElectionMessageEvent;
import org.disalg.met.server.event.NodeCrashEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.*;

public class RAPOSstrategy implements SchedulingStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RAPOSstrategy.class);
    private RAPOSstatistics statistics;
    private Random random;
    List<Event> events;
    List <Event> keepTarackOfScheduled;
    List<Event> schedulable;
    List <Event> scheduled;
    boolean init;

    public RAPOSstrategy(Random random, RAPOSstatistics statistics){
        this.random = random;
        this.statistics = statistics;
        scheduled = new ArrayList<>();
        events = new ArrayList<Event>();
        schedulable = new ArrayList<>();
        keepTarackOfScheduled = new ArrayList<>();
        init = true;
    }

    private boolean nextEventPrepared = false;
    private Event nextEvent;

    @Override
    public void remove(Event event) {
        LOG.warn("Not implementation of removing event: {}", event.toString());
    }

    @Override
    public boolean hasNextEvent(){
        if (!nextEventPrepared){
            prepareNextEvent();
        }
        LOG.debug("hasNextEvent == {}", nextEvent != null);
        return nextEvent != null;
    }

    @Override
    public Event nextEvent(){
        if (!nextEventPrepared){
            prepareNextEvent();
        }
        nextEventPrepared = false;
        return nextEvent;
    }

    private void prepareNextEvent(){
        LOG.debug("Preparing next event...");
        nextEvent = null;
        List <Event> enabled = new ArrayList<>();
        for (Event e: events) {
            if (e.isEnabled()) {
                enabled.add(e);
            }
        }
        if (init){
            for (Event e: enabled) {
                schedulable.add(e);
                }
            init = false;
        }

        while (scheduled.isEmpty() && !enabled.isEmpty()){
            scheduled = RandIndependentSubset();
            for(Event e:scheduled){
                events.remove(e);
            }
            if (!scheduled.isEmpty()) {
                if (!keepTarackOfScheduled.isEmpty()) {
                    schedulable.clear();
                    for (Event e : enabled) {
                        boolean findDependent = false;
                        for (Event eScheduled : keepTarackOfScheduled) {
                            if (isRacy(e, eScheduled)){
                                    findDependent = true;
                                }
                        }
                        if (findDependent)
                            schedulable.add(e);
                    }
                }
                if (schedulable.isEmpty() && !enabled.isEmpty()) {
                    int randIdx = (Math.abs(random.nextInt()))% enabled.size();
                    schedulable.add(enabled.get(randIdx));
                }
                keepTarackOfScheduled.clear();
                keepTarackOfScheduled.addAll(scheduled);
            }
        }
        if (!scheduled.isEmpty() && !enabled.isEmpty()) {
            int randIdx = (Math.abs(random.nextInt()))% scheduled.size();
            nextEvent = scheduled.get(randIdx);
            scheduled.remove(randIdx);
        }
        nextEventPrepared = true;

    }


    @Override
    public void add(final Event event){
        events.add(event);
        LOG.debug("Added event with ID: " + event.getId());
        if (nextEventPrepared && nextEvent == null){
            nextEventPrepared = false;
        }
    }

    private List<Event> RandIndependentSubset()
    {
        List<Event> res = new ArrayList<>();
        for (Event e: schedulable) {
            if (res.isEmpty()){
                res.add(e);
            }
            boolean independent = true;
            for (Event tmp : res) {
                if (isRacy(e, tmp)){
                        independent = false;
                    }
            }
            if (independent){
                boolean pr = random.nextBoolean();
                if (pr) {
                    res.add(e);
                }
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