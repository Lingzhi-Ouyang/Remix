package org.disalg.remix.server;

import org.disalg.remix.server.event.AbstractEvent;

public class NamedEvent extends AbstractEvent {

    private final String name;

    public NamedEvent(final String name, final NamedEventExecutor namedEventExecutor) {
        super(-1, namedEventExecutor);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean execute() {
        final NamedEventExecutor executor = (NamedEventExecutor) getEventExecutor();
        return executor.execute(this);
    }

    @Override
    public String toString() {
        return "NamedEvent{" +
                "name='" + name + '\'' +
                (hasLabel() ? ", label=" + getLabel() : "") +
                '}';
    }
}
