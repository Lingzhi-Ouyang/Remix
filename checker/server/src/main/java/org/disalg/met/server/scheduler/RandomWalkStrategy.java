package org.disalg.met.server.scheduler;

import org.disalg.met.server.statistics.RandomWalkStatistics;
import org.disalg.met.server.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RandomWalkStrategy implements SchedulingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(RandomWalkStrategy.class);

    private final Random random;

    private final RandomWalkStatistics statistics;

    private final Set<Event> events = new HashSet<>();

    public RandomWalkStrategy(final Random random, final RandomWalkStatistics statistics) {
        this.random = random;
        this.statistics = statistics;
    }

    @Override
    public void add(final Event event) {
        LOG.debug("Adding event: {}", event.toString());
        events.add(event);
        if (nextEventPrepared && nextEvent == null) {
            nextEventPrepared = false;
        }
    }

    @Override
    public void remove(Event event) {
        LOG.debug("Removing event: {}", event.toString());
        events.remove(event);
        if (nextEventPrepared) {
            nextEventPrepared = false;
        }
    }

    private boolean nextEventPrepared = false;
    private Event nextEvent = null;

    @Override
    public boolean hasNextEvent() {
        if (!nextEventPrepared) {
            prepareNextEvent();
        }
        return nextEvent != null;
    }

    @Override
    public Event nextEvent() {
        if (!nextEventPrepared) {
            prepareNextEvent();
        }
        nextEventPrepared = false;
        LOG.debug("nextEvent: {}", nextEvent.toString());
        return nextEvent;
    }

    private void prepareNextEvent() {
        final List<Event> enabled = new ArrayList<>();
        LOG.debug("prepareNextEvent: events.size: {}", events.size());
        for (final Event event : events) {
            if (event.isEnabled()) {
                LOG.debug("enabled : {}", event.toString());
                enabled.add(event);
            }
        }
        statistics.reportNumberOfEnabledEvents(enabled.size());

        nextEvent = null;
        if (enabled.size() > 0) {
            final int i = random.nextInt(enabled.size());
            nextEvent = enabled.get(i);
            events.remove(nextEvent);
        }
        nextEventPrepared = true;
    }
}
