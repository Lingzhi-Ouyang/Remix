package org.disalg.met.server.state;

import org.disalg.met.api.SubnodeState;
import org.disalg.met.api.SubnodeType;

public class Subnode {

    private final int id;
    private final int nodeId;
    private final SubnodeType subnodeType;
    private SubnodeState state = SubnodeState.PROCESSING;

    public  Subnode(final int id, final int nodeId, final SubnodeType subnodeType) {
        this.id = id;
        this.nodeId = nodeId;
        this.subnodeType = subnodeType;
    }

    public int getId() {
        return id;
    }

    public int getNodeId() {
        return nodeId;
    }

    public SubnodeType getSubnodeType() {
        return subnodeType;
    }

    public SubnodeState getState() {
        return state;
    }

    public void setState(final SubnodeState state) {
        this.state = state;
    }
}
