package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.disalg.met.api.TestingDef;
import org.disalg.met.api.TestingRemoteService;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.state.LeaderElectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/***
 * The main thread for a node. (corresponding to the QuorumPeer thread in ZooKeeper)
 * All other enhanced threads will call this class' methods when:
 * -> 1. the thread starts to run (registerXXXSubnode)
 * -> 2. the thread is about to exit (deregisterXXXSubnode)
 * -> 3. the thread is about to send / process a message (setXXXSending)
 * -> 4. the thread is allowed to send / process the message (XXXPostSend)
 *      --> Actually, the message is still not sent until this method is done
 *      --> If the msgId == -1, the last existing subnode needs to notify nodeOffline
 * Above methods need to be implemented using the shared variable nodeOnlineMonitor with the critical section
 *
 * The following methods need to use the nodeId
 * -> 1. the thread is about to construct a message payload (constructXXX)
 * ->
 */
public privileged aspect QuorumPeerAspect {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerAspect.class);

    private final TestingRemoteService testingService;

    private int myId;

    private Socket mySock; // for follower

    private boolean syncFinished = false; // for follower

    private boolean newLeaderDone = false; // for follower

    private Integer lastSentMessageId = null;

    private FastLeaderElection.Notification notification;

    private int quorumPeerSubnodeId;

    private int syncSubnodeId = -1;

    private final Object nodeOnlineMonitor = new Object();

    // Manage uncertain number of subnodes
    private boolean quorumPeerSubnodeRegistered = false;
    private boolean workerReceiverSubnodeRegistered = false;
    private boolean workerSenderSubnodeRegistered = false;

    // 1. Use variables for specific subnodes
//    private boolean quorumPeerSending = false;
//    private boolean workerReceiverSending = false;
//    private boolean syncProcessorSending = false;
//    private final Map<Integer, Boolean> learnerHandlerSendingMap = new HashMap<>();

    // 2. Use map for all subnodes
//    private final Map<Integer, Boolean> subnodeSendingMap = new HashMap<>();

    // 3. Use a counter
    private final AtomicInteger sendingSubnodeNum = new AtomicInteger(0);

    // Maintain a subnode list
    private final Map<Long, SubnodeIntercepter> intercepterMap = new ConcurrentHashMap();

    /***
     * THis structure is for subnodes that are multiple of one type in a node.
     */
    public static class SubnodeIntercepter {
        final private String threadName;

        final private int subnodeId;

        final private SubnodeType subnodeType;

        final private TestingRemoteService testingService;

        private Integer lastMsgId = null;

        private boolean sending = false;

        // This is for learner handler sender. After a learner handler has sent UPTODATE, then set it true
        private boolean syncFinished = false;

        @Deprecated
        private final AtomicInteger msgsInQueue = new AtomicInteger(0);

        public SubnodeIntercepter(String threadName, int subnodeId, SubnodeType subnodeType, TestingRemoteService testingService){
            this.threadName = threadName;
            this.subnodeId = subnodeId;
            this.subnodeType = subnodeType;
            this.testingService = testingService;
        }

        public int getSubnodeId() {
            return subnodeId;
        }

        public SubnodeType getSubnodeType() {
            return subnodeType;
        }

        public TestingRemoteService getTestingService() {
            return testingService;
        }

        public AtomicInteger getMsgsInQueue() {
            return msgsInQueue;
        }

        public void setLastMsgId(Integer lastMsgId) {
            this.lastMsgId = lastMsgId;
        }

        public Integer getLastMsgId() {
            return lastMsgId;
        }

        public void setSending(boolean sending) {
            this.sending = sending;
        }

        public void setSyncFinished(boolean syncFinished) {
            this.syncFinished = syncFinished;
        }

        public boolean isSyncFinished() {
            return syncFinished;
        }

        @Override
        public String toString() {
            return "SubnodeIntercepter{" +
                    "threadName='" + threadName + '\'' +
                    ", subnodeId=" + subnodeId +
                    ", subnodeType=" + subnodeType +
                    ", lastMsgId=" + lastMsgId +
                    ", sending=" + sending +
                    ", syncFinished=" + syncFinished +
                    '}';
        }
    }

    public SubnodeIntercepter getIntercepter(long threadId) {
        return intercepterMap.get(threadId);
    }

    public Map<Long, SubnodeIntercepter> getIntercepterMap() {
        return intercepterMap;
    }

    public int getSyncSubnodeId() {
        return syncSubnodeId;
    }

    public void setSyncSubnodeId(int syncSubnodeId) {
        this.syncSubnodeId = syncSubnodeId;
    }

    public TestingRemoteService createRmiConnection() {
        try {
            final Registry registry = LocateRegistry.getRegistry("localhost",2599);
            return (TestingRemoteService) registry.lookup(TestingRemoteService.REMOTE_NAME);
        } catch (final RemoteException e) {
            LOG.error("Couldn't locate the RMI registry.", e);
            throw new RuntimeException(e);
        } catch (final NotBoundException e) {
            LOG.error("Couldn't bind the testing service.", e);
            throw new RuntimeException(e);
        }
    }

    public QuorumPeerAspect() {
        try {
            final Registry registry = LocateRegistry.getRegistry("localhost", 2599);
            testingService = (TestingRemoteService) registry.lookup(TestingRemoteService.REMOTE_NAME);
            LOG.debug("Found the remote testing service.");
        } catch (final RemoteException e) {
            LOG.error("Couldn't locate the RMI registry.", e);
            throw new RuntimeException(e);
        } catch (final NotBoundException e) {
            LOG.error("Couldn't bind the testing service.", e);
            throw new RuntimeException(e);
        }
    }

    public int getMyId() {
        return myId;
    }

    public int getQuorumPeerSubnodeId() {
        return quorumPeerSubnodeId;
    }

    public TestingRemoteService getTestingService() {
        return testingService;
    }

    // For follower QuorumPeer thread
    public void setSyncFinished(boolean syncFinished) {
        this.syncFinished = syncFinished;
    }
    // For follower QuorumPeer thread
    public boolean isSyncFinished() {
        return syncFinished;
    }
    // For follower QuorumPeer thread
    public void setNewLeaderDone(boolean newLeaderDone) {
        this.newLeaderDone = newLeaderDone;
    }
    // For follower QuorumPeer thread
    public boolean isNewLeaderDone() {
        return newLeaderDone;
    }


    // Identify the ID of this node

    pointcut setMyId(long id): set(long org.apache.zookeeper.server.quorum.QuorumPeer.myid) && args(id);

    after(final long id): setMyId(id) {
        myId = (int) id;
        LOG.debug("Set myId = {}", myId);
    }

    // Intercept starting the QuorumPeer thread

    pointcut runQuorumPeer(): execution(* QuorumPeer.run());

    before(): runQuorumPeer() {
        try {
            LOG.debug("-------Thread: {}------", Thread.currentThread().getName());
            LOG.debug("----------------Registering QuorumPeer subnode");
            quorumPeerSubnodeId = testingService.registerSubnode(myId, SubnodeType.QUORUM_PEER);
            LOG.debug("Registered QuorumPeer subnode: id = {}", quorumPeerSubnodeId);
            synchronized (nodeOnlineMonitor) {
                quorumPeerSubnodeRegistered = true;
                if (workerReceiverSubnodeRegistered) {
                    testingService.nodeOnline(myId);
                }
            }
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    after(): runQuorumPeer() {
        try {
            LOG.debug("De-registering QuorumPeer subnode");
            testingService.deregisterSubnode(quorumPeerSubnodeId);
            LOG.debug("-------------------De-registered QuorumPeer subnode\n-------------\n");
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Intercept FastLeaderElection.lookForLeader()

    pointcut lookForLeader(): execution(* org.apache.zookeeper.server.quorum.FastLeaderElection.lookForLeader());

    after() returning (final Vote vote): lookForLeader() {
        try {
            testingService.updateVote(myId, constructVote(vote));
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Intercept message offering within FastLeaderElection, but not within WorkerReceiver

    pointcut offerWithinFastLeaderElection(Object object):
            within(org.apache.zookeeper.server.quorum.FastLeaderElection) && !within(org.apache.zookeeper.server.quorum.FastLeaderElection.Messenger.WorkerReceiver)
            && call(* LinkedBlockingQueue.offer(java.lang.Object))
            && if (object instanceof FastLeaderElection.ToSend)
            && args(object);

    boolean around(final Object object): offerWithinFastLeaderElection(object) {
        final FastLeaderElection.ToSend toSend = (FastLeaderElection.ToSend) object;

        final Set<Integer> predecessorIds = new HashSet<>();
        if (null != notification) {
            predecessorIds.add(notification.getMessageId());
        }
        if (null != lastSentMessageId) {
            predecessorIds.add(lastSentMessageId);
        }

        try {
            LOG.debug("QuorumPeer subnode {} is offering a message with predecessors {}", quorumPeerSubnodeId, predecessorIds.toString());
//            synchronized (nodeOnlineMonitor) {
////                quorumPeerSending = true;
//                subnodeSendingMap.put(quorumPeerSubnodeId, true);
//            }
            setSubnodeSending();
            final String payload = constructPayload(toSend);
            lastSentMessageId = testingService.offerElectionMessage(quorumPeerSubnodeId,
                    (int) toSend.sid, toSend.electionEpoch, (int) toSend.leader, predecessorIds, payload);
            LOG.debug("after offerElectionMessage lastSentMessageId = {}, sendingSubnodeNum: {}", lastSentMessageId, sendingSubnodeNum.get());
            postSend(quorumPeerSubnodeId, lastSentMessageId);
//            synchronized (nodeOnlineMonitor) {
//                quorumPeerSending = false;
//                if (lastSentMessageId == -1) {
//                    // The last existing subnode is responsible to set the node state as offline
//                    if (!workerReceiverSending && !syncProcessorSending) {
//                        testingService.nodeOffline(myId);
//                    }
//                    awaitShutdown(quorumPeerSubnodeId);
//                }
//            }

//            // TODO: to check if the partition happens with around()
            if (lastSentMessageId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message.");
                testingService.setReceivingState(quorumPeerSubnodeId);
                // confirm the return value
                return false;
            }
            return proceed(object);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        } catch (final Exception e) {
            LOG.debug("Uncaught exception when intercepting", e);
            throw new RuntimeException(e); // new added
        }
    }

//    public void setQuorumPeerSending(final int subnodeId) {
//        synchronized (nodeOnlineMonitor) {
////            quorumPeerSending = true;
////            subnodeSendingMap.put(subnodeId, true);
//            sendingSubnodeNum.incrementAndGet();
//        }
//    }

//    public void quorumPeerPostSend(final int subnodeId, final int msgId) throws RemoteException {
//        synchronized (nodeOnlineMonitor) {
////            quorumPeerSending = false;
////            subnodeSendingMap.put(subnodeId, false);
//            sendingSubnodeNum.decrementAndGet();
//            if (lastSentMessageId == -1) {
//                // The last existing subnode is responsible to set the node state as offline
////                if (!workerReceiverSending && !syncProcessorSending) {
//                if (sendingSubnodeNum.get() == 0) {
//                    testingService.nodeOffline(myId);
//                }
//                awaitShutdown(quorumPeerSubnodeId);
//            }
//        }
//    }

    public void setSubnodeSending() {
        synchronized (nodeOnlineMonitor) {
            sendingSubnodeNum.incrementAndGet();
            LOG.debug("add sendingSubnodeNum: {}", sendingSubnodeNum.get());
        }
    }

    public void postSend(final int subnodeId, final int msgId) throws RemoteException {
        synchronized (nodeOnlineMonitor) {
            final int existingSendingSubnodeNum = sendingSubnodeNum.decrementAndGet();
            LOG.debug("-----subnodeId: {}, after decrease sendingSubnodeNum: {}", subnodeId, sendingSubnodeNum.get());
            if (msgId == TestingDef.RetCode.NODE_CRASH) {
                // The last existing subnode is responsible to set the node state as offline
                LOG.debug("-----subnodeId: {}, msgId: {}, existingSendingSubnodeNum: {}", subnodeId, msgId, existingSendingSubnodeNum);
                if (existingSendingSubnodeNum == 0) {
                    LOG.debug("-----going to set nodeOffline by subnodeId: {}, msgId: {}", subnodeId, msgId);
                    testingService.nodeOffline(myId);
                }
                awaitShutdown(subnodeId);
            }
        }
    }

    // only for LEARNER_HANDLER_SENDER
    public void setSubnodeSending(final SubnodeIntercepter intercepter) {
        synchronized (nodeOnlineMonitor) {
            sendingSubnodeNum.incrementAndGet();
            intercepter.sending = true;
            LOG.debug("interceptor: {}, add sendingSubnodeNum: {}", intercepter.getSubnodeId(), sendingSubnodeNum.get());
        }
    }

    // only for LEARNER_HANDLER & LEARNER_HANDLER_SENDER
    public void postSend(final SubnodeIntercepter intercepter, final int subnodeId, final int msgId) throws RemoteException {
        synchronized (nodeOnlineMonitor) {
            int existingSendingSubnodeNum = sendingSubnodeNum.get();
            if (intercepter.sending) {
                intercepter.sending = false;
            }
            if (sendingSubnodeNum.get() > 0) {
                existingSendingSubnodeNum = sendingSubnodeNum.decrementAndGet();
            }
            LOG.debug("-----subnode {} Id: {}, after decrease sendingSubnodeNum: {}",
                    intercepter.subnodeType, subnodeId, sendingSubnodeNum.get());
            if (msgId == TestingDef.RetCode.NODE_CRASH) {
                // The last existing subnode is responsible to set the node state as offline
                LOG.debug("-----subnodeId: {}, msgId: {}, existingSendingSubnodeNum: {}", subnodeId, msgId, existingSendingSubnodeNum);
                if (existingSendingSubnodeNum == 0) {
                    LOG.debug("-----going to set nodeOffline by subnodeId: {}, msgId: {}", subnodeId, msgId);
                    testingService.nodeOffline(myId);
                }
                awaitShutdown(subnodeId);
            }
        }
    }

    public void awaitShutdown(final int subnodeId) {
        try {
            LOG.debug("awaitShutdown. to deregister subnode {}", subnodeId);
            testingService.deregisterSubnode(subnodeId);
            // Going permanently to the wait queue
            nodeOnlineMonitor.wait();
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        } catch (final InterruptedException e) {
            LOG.debug("Interrupted from waiting on nodeOnlineMonitor", e);
        }

    }

    // Intercept polling the FastLeaderElection.recvqueue

    pointcut pollRecvQueue(LinkedBlockingQueue queue):
            withincode(* org.apache.zookeeper.server.quorum.FastLeaderElection.lookForLeader())
            && call(* LinkedBlockingQueue.poll(..))
            && target(queue);

    before(final LinkedBlockingQueue queue): pollRecvQueue(queue) {
        if (queue.isEmpty()) {
            LOG.debug("My FLE.recvqueue is empty, go to RECEIVING state");
            // Going to block here
            try {
                testingService.setReceivingState(quorumPeerSubnodeId);
            } catch (final RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
    }

    after(final LinkedBlockingQueue queue) returning (final FastLeaderElection.Notification notification): pollRecvQueue(queue) {
        this.notification = notification;
        LOG.debug("Received a notification with id = {}", notification.getMessageId());
        try {
            testingService.setReceivingState(quorumPeerSubnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Intercept state update (within QuorumPeer)

    pointcut setPeerState(QuorumPeer.ServerState state):
                    call(* org.apache.zookeeper.server.quorum.QuorumPeer.setPeerState(org.apache.zookeeper.server.quorum.QuorumPeer.ServerState))
                    && args(state);

    after(final QuorumPeer.ServerState state) returning: setPeerState(state) {
        syncFinished = false;
        final LeaderElectionState leState;
        switch (state) {
            case LEADING:
                leState = LeaderElectionState.LEADING;
                break;
            case FOLLOWING:
                leState = LeaderElectionState.FOLLOWING;
                break;
            case OBSERVING:
                leState = LeaderElectionState.OBSERVING;
                break;
            case LOOKING:
            default:
                leState = LeaderElectionState.LOOKING;
                break;
        }
        try {
            LOG.debug("----------setPeerState2: Node {} state: {}", myId, state);
            testingService.updateLeaderElectionState(myId, leState);
            if(leState == LeaderElectionState.LOOKING){
                syncFinished = false;
                newLeaderDone = false;
                testingService.updateVote(myId, null);
            }
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public String constructPayload(final FastLeaderElection.ToSend toSend) {
        return "from=" + myId +
                ", to=" + toSend.sid +
                ", leader=" + toSend.leader +
                ", state=" + toSend.state +
                ", zxid=0x" + Long.toHexString(toSend.zxid) +
                ", electionEpoch=" + toSend.electionEpoch +
                ", peerEpoch=" + toSend.peerEpoch;
    }

    private org.disalg.met.api.state.Vote constructVote(final Vote vote) {
        return new org.disalg.met.api.state.Vote(vote.getId(), vote.getZxid(), vote.getElectionEpoch(), vote.getPeerEpoch());
    }

    // Node state management

    /***
     * WorkerReceiver
     */
    public int registerWorkerReceiverSubnode() {
        final int workerReceiverSubnodeId;
        try {
            LOG.debug("Registering WorkerReceiver subnode");
            workerReceiverSubnodeId = testingService.registerSubnode(myId, SubnodeType.WORKER_RECEIVER);
            LOG.debug("Registered WorkerReceiver subnode: id = {}", workerReceiverSubnodeId);
            synchronized (nodeOnlineMonitor) {
                workerReceiverSubnodeRegistered = true;
                if (quorumPeerSubnodeRegistered) {
                    testingService.nodeOnline(myId);
                }
            }
            return workerReceiverSubnodeId;
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public void deregisterWorkerReceiverSubnode(final int workerReceiverSubnodeId) {
        try {
            LOG.debug("De-registering WorkerReceiver subnode");
            testingService.deregisterSubnode(workerReceiverSubnodeId);
            LOG.debug("De-registered WorkerReceiver subnode");
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

//    public void setWorkerReceiverSending() {
//        synchronized (nodeOnlineMonitor) {
//            workerReceiverSending = true;
//        }
//    }
//
//    public void workerReceiverPostSend(final int subnodeId, final int msgId) throws RemoteException {
//        synchronized (nodeOnlineMonitor) {
//            workerReceiverSending = false;
//            // msgId == -1 means that the sending node is about to be shutdown
//            if (msgId == -1) {
//                // Ensure that other threads are all finished
//                // The last existing subnode is responsible to set the node state as offline
//                if (!quorumPeerSending && !syncProcessorSending) {
//                    testingService.nodeOffline(myId);
//                }
//                awaitShutdown(subnodeId);
//            }
//        }
//    }


    /***
     * WorkerSender
     */
    public int registerWorkerSenderSubnode() {
        final int workerSenderSubnodeId;
        try {
            LOG.debug("Registering WorkerSender subnode");
            workerSenderSubnodeId = testingService.registerSubnode(myId, SubnodeType.WORKER_SENDER);
            LOG.debug("Registered WorkerSender subnode: id = {}", workerSenderSubnodeId);
//            synchronized (nodeOnlineMonitor) {
//                workerSenderSubnodeRegistered = true;
//                if (workerReceiverSubnodeRegistered) {
//                    testingService.nodeOnline(myId);
//                }
//            }
            return workerSenderSubnodeId;
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public void deregisterWorkerSenderSubnode(final int workerSenderSubnodeId) {
        try {
            LOG.debug("De-registering WorkerSender subnode");
            testingService.deregisterSubnode(workerSenderSubnodeId);
            LOG.debug("De-registered WorkerSender subnode");
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }


    /***
     * for threads whose subnodeType is unique in a process, e.g. SyncRequestProcessor & CommitProcessor
     */
    @Deprecated
    public int registerSubnode(final TestingRemoteService testingService, final SubnodeType subnodeType) {
        try {
            LOG.debug("Found the remote testing service. Registering {} subnode", subnodeType);
            final int subnodeId = testingService.registerSubnode(myId, subnodeType);
            LOG.debug("Finish registering {} subnode: id = {}", subnodeType, subnodeId);
            return subnodeId;
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception.", e);
            throw new RuntimeException(e);
        }
    }

    public void deregisterSubnode(final TestingRemoteService testingService, final int subnodeId, final SubnodeType subnodeType) {
        try {
            LOG.debug("De-registering {} subnode {}", subnodeType, subnodeId);
            if (subnodeId < 0) {
                LOG.debug("{} subnodeId == {}", subnodeType, subnodeId);
                return;
            }
            testingService.deregisterSubnode(subnodeId);
            LOG.debug("Finish de-registering {} subnode {}", subnodeType, subnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }


    /***
     * The following registerSubnode() and deregisterSubnode() methods are for threads that will be run more than one
     * in a node such as LearnerHandler and LearnerHandlerSender
     * These subnode info will be stored using the SubnodeInterceptor structure
     *
     * Usage: SyncRequestProcessor & CommitProcessor & LearnerHandler & LearnerHandlerSender
     *
     * @param threadId
     * @param threadName
     * @param subnodeType
     * @return
     */

    public SubnodeIntercepter registerSubnode(final long threadId, final String threadName, final SubnodeType subnodeType){
        try {
            TestingRemoteService testingService = createRmiConnection();
            LOG.debug("Found the remote testing service. Registering {} subnode", subnodeType);
            final int subnodeId = testingService.registerSubnode(myId, subnodeType);
            LOG.debug("Finish registering {} subnode: id = {}", subnodeType, subnodeId);
            SubnodeIntercepter intercepter =
                    new SubnodeIntercepter(threadName, subnodeId, subnodeType, testingService);
            intercepterMap.put(threadId, intercepter);
            return intercepter;
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception.", e);
            throw new RuntimeException(e);
        }
    }

    public void deregisterSubnode(final long threadId) {
        try {
            SubnodeIntercepter intercepter = intercepterMap.get(threadId);
            final SubnodeType subnodeType = intercepter.getSubnodeType();
            final int subnodeId = intercepter.getSubnodeId();
            LOG.debug("De-registering {} subnode {}", subnodeType, subnodeId);
            if (subnodeId < 0) {
                LOG.debug("{} subnodeId == {}, threadId: {}", subnodeType, subnodeId, threadId);
                return;
            }
            testingService.deregisterSubnode(subnodeId);
            LOG.debug("Finish de-registering {} subnode {}", subnodeType, subnodeId);
//            // for LearnerHandlerSender
//            synchronized (nodeOnlineMonitor) {
//                if (intercepter.sending) {
//                    intercepter.sending = false;
//                    if (sendingSubnodeNum.get() > 0){
//                        final int existingSendingSubnodeNum = sendingSubnodeNum.decrementAndGet();
//                        LOG.debug("-----subnode {} Id: {}, decrease sendingSubnodeNum: {}",
//                                subnodeType, subnodeId, sendingSubnodeNum.get());
//                    }
//                }
//            }
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // intercept the address of the socket of the learner
    // Identify the ID of this node

    pointcut setMySock(Learner learner):
            call(* Learner.connectToLeader(..)) && target(learner);

    after(final Learner learner): setMySock(learner) {
        try {
            mySock = learner.getSocket();
            LOG.debug("getLocalSocketAddress = {}", mySock.getLocalSocketAddress());
            testingService.registerFollowerSocketInfo(myId, mySock.getLocalSocketAddress().toString());
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    //TODO: unregister socket address

//    pointcut newSock(Socket socket):
//            set(Socket Learner.sock) && args(socket);
//
//    after(final Socket sock): newSock(sock) {
//        mySock = sock;
//        LOG.debug("getInetAddress = {}", sock.getInetAddress());
//        LOG.debug("getLocalAddress = {}", sock.getLocalAddress());
//        LOG.debug("getLocalSocketAddress = {}, {}", sock.getLocalSocketAddress(), sock.getLocalPort());
//    }

//    // intercept the initialization of a learner handler for the leader node
//
//    pointcut newLearnerHandler(Socket sock, Leader leader):
//            initialization(LearnerHandler.new(Socket, Leader))
//            && args(sock, leader);
//
//    after(final Socket sock, final Leader leader): newLearnerHandler(sock, leader) {
//        LOG.debug("getLocalSocketAddress = {}", sock.getRemoteSocketAddress());
//    }

    // intercept the effects of network partition
//    pointcut followerProcessPacket

    public void addToQueuedPackets(final long threadId, final Object object) {
        final AtomicInteger msgsInQueuedPackets = intercepterMap.get(threadId).getMsgsInQueue();
        msgsInQueuedPackets.incrementAndGet();
        final String payload = packetToString((QuorumPacket) object);
        LOG.debug("learnerHandlerSubnodeId: {}----------packet: {}", intercepterMap.get(threadId).getSubnodeId(), payload);
        LOG.debug("----------addToQueuedPackets(). msgsInQueuedPackets.size: {}", msgsInQueuedPackets.get());
    }

    public String packetToString(QuorumPacket p) {
        if (p == null) return "null";
        String type;
        switch (p.getType()) {
            case Leader.ACK:
                type = "ACK";
                break;
            case Leader.COMMIT:
                type = "COMMIT";
                break;
            case Leader.FOLLOWERINFO:
                type = "FOLLOWERINFO";
                break;
            case Leader.NEWLEADER:
                type = "NEWLEADER";
                break;
            case Leader.PING:
                type = "PING";
                break;
            case Leader.PROPOSAL:
                type = "PROPOSAL";
                break;
            case Leader.REQUEST:
                type = "REQUEST";
                break;
            case Leader.REVALIDATE:
                type = "REVALIDATE";
                break;
            case Leader.UPTODATE:
                type = "UPTODATE";
                break;
            default:
                type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            // TODO: acquire receiving node from remote socket
            entry = "type=" + type +
                    ", typeId=" + p.getType() +
                    ", sendingNode=" + myId +
                    ", zxid=0x" + Long.toHexString(p.getZxid());
        }
        return entry;
    }

    public String constructRequest(final Request request) {
        return "Node=" + myId +
                ", sessionId=" + request.sessionId +
                ", cxid=" + request.cxid +
                ", zxid=0x" + Long.toHexString(request.zxid) +
                ", typeId=" + request.type;
    }

    // Identify the last processed zxid of this node

    pointcut writeLongToFile(String name, long epoch): execution(void QuorumPeer.writeLongToFile(String, long)) && args(name, epoch);

    after(final String name, final long epoch) returning: writeLongToFile(name, epoch) {
        try {
            LOG.debug("-------nodeId: {}, after writeLongToFile: set {} = 0x{}", myId, name, Long.toHexString(epoch));
            lastSentMessageId = testingService.writeLongToFile(myId, name, epoch);
            if (name.equals("currentEpoch")) {
                // increase sendingSubnodeNum here for later decreasing
                setSubnodeSending();
//                final int lastRequestId = testingService.offerLocalEvent(quorumPeerSubnodeId, SubnodeType.QUORUM_PEER,
//                        epoch, "setCurrentEpoch", TestingDef.MessageType.NEWLEADER);
                LOG.debug("lastSentMessageId = {}", lastSentMessageId);
                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
                postSend(quorumPeerSubnodeId, lastSentMessageId);
            }
        } catch (final Exception e) {
            LOG.error("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

//    /***
//     * intercept leader calling waitForEpochAck method
//     */
//    pointcut waitForEpochAck(long id):
//            withincode(* Leader.lead()) &&
//                    call(* Leader.waitForEpochAck(long, *)) && args(id, *);
//
//    before(long id): waitForEpochAck(id) {
//        LOG.debug("before waitForEpochAck, sid: {}", id);
//
//        try {
//
//            // before offerMessage: increase sendingSubnodeNum
//            setSubnodeSending();
//            final int lastPacketId = testingService
//                    .offerLocalEvent(quorumPeerSubnodeId, SubnodeType.QUORUM_PEER, id, null, -2);
//            LOG.debug("waitForEpochAck lastPacketId = {}", lastPacketId);
//            postSend(quorumPeerSubnodeId, lastPacketId);
//            // Trick: set RECEIVING state here
//            testingService.setReceivingState(quorumPeerSubnodeId);
//        } catch (RemoteException e) {
//            LOG.debug("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
//    }





}