package org.disalg.met.api.state;

public interface GlobalState {

    boolean isOnline(long node);

    void setOnline(long node, boolean online);

    boolean isFinal();

    void registerGobalStateFinalListener(GlobalStateFinalListener listener);

    void notifyGlobalStateFinalListeners();

}
