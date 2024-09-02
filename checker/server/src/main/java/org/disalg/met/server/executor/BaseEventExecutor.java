package org.disalg.met.server.executor;

import org.disalg.met.server.event.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public class BaseEventExecutor {

    public boolean execute(final NodeCrashEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final NodeStartEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final ClientRequestEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final ElectionMessageEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final FollowerToLeaderMessageEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final LocalEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final LeaderToFollowerMessageEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final PartitionStartEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final PartitionStopEvent event) throws IOException {
        throw new NotImplementedException();
    }
}
