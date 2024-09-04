package org.disalg.remix.api.state;

public interface GlobalStateUpdater<G extends GlobalState> {

    void update(G globalState);

}
