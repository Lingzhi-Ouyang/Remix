package org.disalg.met.server.event;

import org.disalg.met.api.TestingDef;
import org.disalg.met.server.executor.BaseEventExecutor;

import java.util.*;

public abstract class AbstractEvent implements Event {

    private final BaseEventExecutor eventExecutor;

    private final int id;

    private final List<Event> directPredecessors = new ArrayList<>();

    public AbstractEvent(final int id, final BaseEventExecutor eventExecutor) {
        this.id = id;
        this.eventExecutor = eventExecutor;
        this.flag = TestingDef.RetCode.UNDEFINED;
    }

    @Override
    public int getId() {
        return id;
    }

    private Integer flag;

    @Override
    public int getFlag() {
        return flag;
    }

    @Override
    public void setFlag(int flag) {
        this.flag = flag;
    }

    public BaseEventExecutor getEventExecutor() {
        return eventExecutor;
    }

    @Override
    public void addDirectPredecessor(final Event event) {
        directPredecessors.add(event);
    }

    @Override
    public void addAllDirectPredecessors(final Collection<? extends Event> col) {
        directPredecessors.addAll(col);
    }

    @Override
    public List<Event> getDirectPredecessors() {
        return Collections.unmodifiableList(directPredecessors);
    }

    @Override
    public boolean happensBefore(final Event event) {
        final Set<Event> visited = new HashSet<>();
        final Queue<Event> queue = new ArrayDeque<>(event.getDirectPredecessors());
        while (!queue.isEmpty()) {
            final Event head = queue.remove();
            if (visited.contains(head)) {
                continue;
            }
            if (this.equals(head)) {
                return true;
            }
            queue.addAll(head.getDirectPredecessors());
            visited.add(head);
        }
        return false;
    }

    @Override
    public boolean isEnabled() {
        for (final Event predecessor : getDirectPredecessors()) {
            if (!predecessor.isExecuted()) {
                return false;
            }
        }
        return true;
    }

    private boolean executed = false;

    public void setExecuted() {
        executed = true;
    }

    @Override
    public boolean isExecuted() {
        return executed;
    }

    private Integer label;

    @Override
    public boolean hasLabel() {
        return label != null;
    }

    @Override
    public int getLabel() {
        return label;
    }

    @Override
    public void setLabel(int label) {
        this.label = label;
    }

    protected String getDirectPredecessorsString() {
        final StringBuilder sb = new StringBuilder("[");
        String separator = "";
        for (final Event event : getDirectPredecessors()) {
            sb.append(separator);
            sb.append(event.getId());
            separator = ", ";
        }
        sb.append("]");
        return sb.toString();
    }

    protected String getLabelString() {
        return (hasLabel() ? ", label=" + getLabel() : "");
    }
}
