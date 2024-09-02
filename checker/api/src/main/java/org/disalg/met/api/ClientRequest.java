package org.disalg.met.api;

import org.disalg.met.api.state.ClientRequestType;

import java.io.IOException;

public class ClientRequest {

    private final ClientRequestType type;
    private final int clientId;
    private final String key = "/test";
    private String data = null;
    private String result = null;
    private boolean stop = false;


    public ClientRequest(final int id, final int clientId, ClientRequestType type) {
        this.type = type;
        this.clientId = clientId;
        if (ClientRequestType.SET_DATA.equals(type)){
            this.data = String.valueOf(id);
        }
    }

    public ClientRequest(final int id, final int clientId, ClientRequestType type, final String data) {
        this.type = type;
        this.clientId = clientId;
        if (ClientRequestType.SET_DATA.equals(type)){
            this.data = data;
        }
    }

    public boolean isStop() {
        return stop;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    public ClientRequestType getType() {
        return type;
    }

    public int getClientId() {
        return clientId;
    }

    public String getKey() {
        return key;
    }

    public String getData() {
        return data;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
