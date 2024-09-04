package org.disalg.remix.api.state;

public interface PropertyVerifier<G extends GlobalState> {

    void verify(G globalState);

}
