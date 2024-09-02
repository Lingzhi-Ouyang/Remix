package org.disalg.met.server.event;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public interface Event {

    int getId();
    
    boolean execute() throws IOException;

    boolean isExecuted();

    void addDirectPredecessor(Event event);

    void addAllDirectPredecessors(Collection<? extends Event> col);

    List<Event> getDirectPredecessors();

    boolean happensBefore(Event event);

    boolean isEnabled();

    boolean hasLabel();

    int getLabel();

    void setLabel(int label);

    int getFlag();

    void setFlag(int flag);
}
