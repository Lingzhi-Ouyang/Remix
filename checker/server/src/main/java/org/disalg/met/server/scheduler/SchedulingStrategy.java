package org.disalg.met.server.scheduler;

import org.disalg.met.server.event.Event;

public interface SchedulingStrategy {

    void add(Event event);

    void remove(Event event);

    boolean hasNextEvent();

    Event nextEvent();

}
