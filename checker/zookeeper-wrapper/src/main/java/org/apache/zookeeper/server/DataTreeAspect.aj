package org.apache.zookeeper.server;

import org.disalg.remix.api.RemoteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public aspect DataTreeAspect {
    private static final Logger LOG = LoggerFactory.getLogger(DataTreeAspect.class);

    private final RemoteService remoteService;

    private long myLastProcessedZxid;

    private int myId;

    public DataTreeAspect() {
        try {
            final Registry registry = LocateRegistry.getRegistry(2599);
            remoteService = (RemoteService) registry.lookup(RemoteService.REMOTE_NAME);
            LOG.debug("Found the remote replay service.");
        } catch (final RemoteException e) {
            LOG.error("Couldn't locate the RMI registry.", e);
            throw new RuntimeException(e);
        } catch (final NotBoundException e) {
            LOG.error("Couldn't bind the replay service.", e);
            throw new RuntimeException(e);
        }
    }

    public RemoteService getRemoteService() {
        return remoteService;
    }

    public int getMyId() {
        return myId;
    }

    // Identify the ID of this node

    pointcut setMyId(long id): set(long quorum.QuorumPeer.myid) && args(id);

    after(final long id): setMyId(id) {
        myId = (int) id;
        LOG.debug("Set myId = {}", myId);
    }

    // Identify the last processed zxid of this node

    pointcut setMyLastProcessedZxid(long zxid): set(long DataTree.lastProcessedZxid) && args(zxid);

    after(final long zxid): setMyLastProcessedZxid(zxid) {
        myLastProcessedZxid = zxid;
        try {
            LOG.debug("-------nodeId: {}, Set myLastProcessedZxid = 0x{}", myId, Long.toHexString(myLastProcessedZxid));
            remoteService.updateLastProcessedZxid(myId, myLastProcessedZxid);
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }


}
