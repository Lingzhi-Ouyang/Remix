package org.disalg.remix.server.scheduler;

import org.disalg.remix.server.event.Event;

public interface SchedulingStrategy {

    void add(Event event);

    void remove(Event event);

    boolean hasNextEvent();

    Event nextEvent();

}
