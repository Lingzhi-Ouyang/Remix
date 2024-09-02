package org.disalg.met.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.disalg.met.api.ClientRequest;
import org.disalg.met.api.TestingRemoteService;
import org.disalg.met.api.state.ClientRequestType;
import org.disalg.met.server.event.ClientRequestEvent;
import org.disalg.met.server.executor.ZooKeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

public class ZooKeeperClientProcess {
//    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClientProcess.class);
//
//    private final TestingRemoteService testingService;
//
//    private final int clientId;
//
//    volatile boolean ready;
//
//    volatile boolean stop;
//
//    private String serverList = "127.0.0.1:4002,127.0.0.1:4001,127.0.0.1:4000";
//
//    private ZooKeeperClient zooKeeperClient;
//
//    public ZooKeeperClientProcess(final int clientId, final String serverList){
//        testingService = createRmiConnection();
//        this.ready = false;
//        this.stop = true;
//        this.serverList = serverList;
//        this.clientId = clientId;
//    }
//
//    public boolean isReady() {
//        return ready;
//    }
//
//    public boolean isStop() {
//        return stop;
//    }
//
//    public boolean init() {
//        this.ready = false;
//        int retry = 5;
//        while (retry > 0) {
//            try {
//                zooKeeperClient = new ZooKeeperClient(serverList, true);
//                zooKeeperClient.getCountDownLatch().await();
//
//                LOG.debug("----------Init: create key=test-------");
//                zooKeeperClient.create();
//
//                return true;
//            } catch (InterruptedException | KeeperException | IOException e) {
//                LOG.debug("----- caught {} during client session initialization", e.toString());
//                e.printStackTrace();
//                retry--;
//            }
//        }
//        return false;
//    }
//
//    public void shutdown(){
//        this.ready = false;
//        this.stop = true;
//    }
//
//    public void run() {
//        stop = false;
//        while (!stop) {
//            if (init()) {
//                this.ready = true;
//            } else {
//                LOG.debug("client {} init failure!", clientId);
//                return;
//            }
//            String lastResult = null;
//            while (ready && !stop) {
//                try {
//                    final ClientRequest clientRequest = testingService.getNextClientRequest(clientId, lastResult);
//                    if (clientRequest.isStop()) {
//                        LOG.debug("Receiving {}. client {} is going to stop", clientRequest, clientId);
//                        stop = true;
//                        continue;
//                    }
//                    String lastResult = process(clientRequest);
//                } catch (InterruptedException | KeeperException | RemoteException e) {
//                    e.printStackTrace();
//                    break;
//                }
//            }
//            try {
//                zooKeeperClient.close();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            this.ready = false;
//        }
//    }
//
//    private String process(ClientRequest clientRequest) throws InterruptedException, KeeperException {
//        ClientRequestType type = clientRequest.getType();
//        switch (type) {
//            case GET_DATA:
//                String result = zooKeeperClient.getData();
//                LOG.debug("---done wait for GET_DATA result: {}", result);
//                return result;
//            case SET_DATA:
//                // always return immediately
//                zooKeeperClient.setData(clientRequest.getData());
//                clientRequest.setResult(clientRequest.getData());
//                return "SET_DATA_DONE";
//        }
//        return "null";
//    }
//
//    public TestingRemoteService createRmiConnection() {
//        try {
//            final Registry registry = LocateRegistry.getRegistry(2599);
//            return (TestingRemoteService) registry.lookup(TestingRemoteService.REMOTE_NAME);
//        } catch (final RemoteException e) {
//            LOG.error("Couldn't locate the RMI registry.", e);
//            throw new RuntimeException(e);
//        } catch (final NotBoundException e) {
//            LOG.error("Couldn't bind the testing service.", e);
//            throw new RuntimeException(e);
//        }
//    }
//
//    public static void main(String[] args)
//            throws KeeperException, IOException, InterruptedException {
//        ZooKeeperClientProcess main = new ZooKeeperClientProcess();
//        main.run();
//    }

}
