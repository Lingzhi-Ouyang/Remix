package org.disalg.met.api.state;

public interface GlobalStateUpdater<G extends GlobalState> {

    void update(G globalState);

}
