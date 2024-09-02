package org.disalg.met.api.state;

public interface PropertyVerifier<G extends GlobalState> {

    void verify(G globalState);

}
