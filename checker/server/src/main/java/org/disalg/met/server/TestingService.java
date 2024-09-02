package org.disalg.met.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.disalg.met.api.*;
import org.disalg.met.api.configuration.SchedulerConfiguration;
import org.disalg.met.server.checker.CommittedLogVerifier;
import org.disalg.met.server.event.*;
import org.disalg.met.server.executor.*;
import org.disalg.met.server.predicate.*;
import org.disalg.met.server.scheduler.*;
import org.disalg.met.server.statistics.*;
import org.disalg.met.api.configuration.SchedulerConfigurationException;
import org.disalg.met.api.state.ClientRequestType;
import org.disalg.met.api.state.LeaderElectionState;
import org.disalg.met.api.state.Vote;
import org.disalg.met.server.checker.GetDataVerifier;
import org.disalg.met.server.checker.LeaderElectionVerifier;
import org.disalg.met.server.checker.TraceVerifier;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TestingService implements TestingRemoteService {

    private static final Logger LOG = LoggerFactory.getLogger(TestingService.class);

    @Autowired
    private SchedulerConfiguration schedulerConfiguration;

    @Autowired
    private Ensemble ensemble;

    private final Map<Integer, ClientProxy> clientMap = new HashMap<>();
    private final Set<String> keySet = new HashSet<>();

    // strategy
    private SchedulingStrategy schedulingStrategy;

    // External event executors
    private NodeStartExecutor nodeStartExecutor;
    private NodeCrashExecutor nodeCrashExecutor;
    private ClientRequestExecutor clientRequestWaitingResponseExecutor;
    private ClientRequestExecutor clientRequestExecutor;
    private PartitionStartExecutor partitionStartExecutor;
    private PartitionStopExecutor partitionStopExecutor;

    // Internal event executors
    private ElectionMessageExecutor electionMessageExecutor; // for election
    private LocalEventExecutor localEventExecutor; // for logging
    private LeaderToFollowerMessageExecutor leaderToFollowerMessageExecutor; // for learnerHandler-follower
    private FollowerToLeaderMessageExecutor followerToLeaderMessageExecutor; // for follower-leader

    // statistics
    private Statistics statistics;

    // checker
    private TraceVerifier traceVerifier;
    private LeaderElectionVerifier leaderElectionVerifier;
    private GetDataVerifier getDataVerifier;
    private CommittedLogVerifier committedLogVerifier;

    // writer
    private FileWriter statisticsWriter;
    private FileWriter executionWriter;
    private FileWriter bugReportWriter;
    private FileWriter matchReportWriter;

    private final Object controlMonitor = new Object();

    // For indication of client initialization
    private boolean clientInitializationDone = true;
    private final Map<Integer, Boolean> readRecordIntercepted = new HashMap<>();

    // For network partition
    private List<List<Boolean>> partitionMap = new ArrayList<>();

    // node management
    private final List<NodeState> nodeStates = new ArrayList<>();
    private final List<Phase> nodePhases = new ArrayList<>();

    private final List<Subnode> subnodes = new ArrayList<>();
    private final List<Set<Subnode>> subnodeSets = new ArrayList<>();

    private final Map<Integer, Integer> subnodeMap = new HashMap<>();

    private final List<String> followerSocketAddressBook = new ArrayList<>();
    private final List<Integer> followerLearnerHandlerMap = new ArrayList<>();
    private final List<Integer> followerLearnerHandlerSenderMap = new ArrayList<>();

    private final List<NodeStateForClientRequest> nodeStateForClientRequests = new ArrayList<>();

    private final Set<Integer> participants = new HashSet<>();

    /***
     * properties to check
     */

    // record each node's vote for check after election
    private final List<Vote> votes = new ArrayList<>();
    // record each node's role for check after election
    private final List<LeaderElectionState> leaderElectionStates = new ArrayList<>();

    // record each node's lastProcessedZxid
    private final List<Long> lastProcessedZxids = new ArrayList<>();

    // record each node's currentEpoch
    private final List<Long> acceptedEpochs = new ArrayList<>();

    // record each node's currentEpoch
    private final List<Long> currentEpochs = new ArrayList<>();

    // pending proposals & commits in SYNC
    private final List<List<Long>> pendingProposalsInSync = new ArrayList<>();
    private final List<List<Long>> pendingCommitsInSync = new ArrayList<>();

    // zxid map from model to code
    private final Map<Long, Long> modelToCodeZxidMap = new HashMap<>();

    // record all nodes' lastProcessedZxid history
    // Note: do not record zxid whose counter is 0 when the epoch >= 1
    // but set 0 as its first record for check convenience
    private final List<List<Long>> allZxidRecords = new ArrayList<>();

    // record the returned values of GetData according to the timeline
    private final List<String> returnedDataList = new ArrayList<>();

    // record the committed / applied history (may be visible by clients)
    // Note: do not record zxid whose counter is 0 when the epoch >= 1 (since this zxid does not map to a real commit)
    // ( which is firstly produced by the leader during discovery phase in each epoch)
    // only record the committed zxid during broadcast phase
    // but set 0 as its first record for check convenience
    private final List<Long> lastCommittedZxid = new ArrayList<>();

    // record the number of followers.
    // For post-predicate of ElectionAndDiscovery event
    private final Map<Integer, Integer> leaderSyncFollowerCountMap = new HashMap<>();

    // record the number of nodes that have logged the proposal with specific zxid.
    // For pre-predicate of leader's commit message event
    private final Map<Long, Integer> zxidSyncedMap = new HashMap<>();

    // record the number of nodes that have committed with specific zxid.
    private final Map<Long, Integer> zxidToCommitMap = new HashMap<>();

    // record the sync type : DIFF / TRUNC / SNAP
    private final List<Integer> syncTypeList = new ArrayList<>();

    public boolean traceMatched;
    public boolean tracePassed;

    // event
    private final AtomicInteger eventIdGenerator = new AtomicInteger();

    private final AtomicInteger clientIdGenerator = new AtomicInteger();

    // for event dependency
    private final Map<Integer, ElectionMessageEvent> messageEventMap = new HashMap<>();
//    private final Map<Integer, LeaderToFollowerMessageEvent> followerMessageEventMap = new HashMap<>();

    private final List<NodeStartEvent> lastNodeStartEvents = new ArrayList<>();
    // TODO: firstMessage should be renewed whenever election occurs.
    private final List<Boolean> firstMessage = new ArrayList<>();

    private int messageInFlight;

    private int logRequestInFlight;

    public void setMessageInFlight(int messageInFlight) {
        this.messageInFlight = messageInFlight;
    }

    public List<Phase> getNodePhases() {
        return nodePhases;
    }

    public List<List<Boolean>> getPartitionMap() {
        return partitionMap;
    }

    public Set<Integer> getParticipants() {
        return participants;
    }

    public void setPartitionMap(List<List<Boolean>> partitionMap) {
        this.partitionMap = partitionMap;
    }

    public List<List<Long>> getAllZxidRecords() {
        return allZxidRecords;
    }

    public Map<Long, Long> getModelToCodeZxidMap() {
        return modelToCodeZxidMap;
    }

    public List<String> getReturnedDataList() {
        return returnedDataList;
    }

    public Map<Long, Integer> getZxidSyncedMap() {
        return zxidSyncedMap;
    }

    public Map<Integer, Integer> getLeaderSyncFollowerCountMap() {
        return leaderSyncFollowerCountMap;
    }

    public Map<Long, Integer> getZxidToCommitMap() {
        return zxidToCommitMap;
    }

    public Object getControlMonitor() {
        return controlMonitor;
    }

    public long getLastProcessedZxid(int nodeId) {
        return lastProcessedZxids.get(nodeId);
    }

    public long getCurrentEpoch(int nodeId) {
        return currentEpochs.get(nodeId);
    }

    public long getAcceptedEpoch(int nodeId) {
        return acceptedEpochs.get(nodeId);
    }

    public List<Long> getLastCommittedZxid() {
        return lastCommittedZxid;
    }

    public List<Integer> getSyncTypeList() {
        return syncTypeList;
    }

    public int getSyncType(final int idx) {
        return syncTypeList.get(idx);
    }

    public List<NodeStateForClientRequest> getNodeStateForClientRequests() {
        return nodeStateForClientRequests;
    }

    public NodeStateForClientRequest getNodeStateForClientRequests(int nodeId) {
        return nodeStateForClientRequests.get(nodeId);
    }

    public List<NodeState> getNodeStates() {
        return nodeStates;
    }

    public List<Subnode> getSubnodes() {
        return subnodes;
    }

    public List<Set<Subnode>> getSubnodeSets() {
        return subnodeSets;
    }

    public List<String> getFollowerSocketAddressBook() {
        return followerSocketAddressBook;
    }

    public List<Integer> getFollowerLearnerHandlerMap() {
        return followerLearnerHandlerMap;
    }

    public int getFollowerLearnerHandlerMap(int followerId) {
        return followerLearnerHandlerMap.get(followerId);
    }

    public List<Integer> getFollowerLearnerHandlerSenderMap() {
        return followerLearnerHandlerSenderMap;
    }

    public Integer getFollowerLearnerHandlerSenderMap(int followerId) {
        return followerLearnerHandlerSenderMap.get(followerId);
    }

    public List<Vote> getVotes() {
        return votes;
    }

    public List<LeaderElectionState> getLeaderElectionStates() {
        return leaderElectionStates;
    }

    public LeaderElectionState getLeaderElectionState(int node) {
        return leaderElectionStates.get(node);
    }

    public List<Boolean> getFirstMessage() {
        return firstMessage;
    }

    public NodeStartExecutor getNodeStartExecutor() {
        return nodeStartExecutor;
    }

    public NodeCrashExecutor getNodeCrashExecutor() {
        return nodeCrashExecutor;
    }

    public ClientProxy getClientProxy(final int clientId) {
        assert clientMap.containsKey(clientId);
        return clientMap.get(clientId);
    }

    public LinkedBlockingQueue<ClientRequestEvent> getRequestQueue(final int clientId) {
        return clientMap.get(clientId).getRequestQueue();
    }

    public SchedulerConfiguration getSchedulerConfiguration() {
        return schedulerConfiguration;
    }

    public void loadConfig(final String[] args) throws SchedulerConfigurationException {
        schedulerConfiguration.load(args);
    }

    public SchedulingStrategy getSchedulingStrategy() {
        return schedulingStrategy;
    }

    /***
     * The core process the scheduler by specifying each step pf zk-3911
     * @throws SchedulerConfigurationException
     * @throws IOException
     */
    public void start() throws SchedulerConfigurationException, IOException {
        LOG.debug("Starting the testing service");
        long startTime = System.currentTimeMillis();

        for (int executionId = 1; executionId <= schedulerConfiguration.getNumExecutions(); ++executionId) {
            ensemble.configureEnsemble(String.valueOf(executionId));
            configureSchedulingStrategy(executionId);

            int totalExecuted = 0;

            executionWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + executionId + File.separator + schedulerConfiguration.getExecutionFile());
            statisticsWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + executionId + File.separator + schedulerConfiguration.getStatisticsFile());

            // configure the execution before first election
            configureNextExecution();
            // start the ensemble at the beginning of the execution
            ensemble.startEnsemble();
            executionWriter.write("-----Initialization cost time: " + (System.currentTimeMillis() - startTime) + "\n\n");

            // PRE: first election
            totalExecuted = scheduleElection(null, null,null, totalExecuted);

            // Only the success of the last verification will lead to the following test
            // o.w. just report bug

            // 1. trigger DIFF
            int client1 = generateClientId();
//            createClient(client1, true, "127.0.0.1:4002");
            establishSession(client1, true, "127.0.0.1:4002");

            // Restart the leader will disconnect the client session!
//            nodeStartExecutor = new NodeStartExecutor(this, 1);
//            nodeCrashExecutor = new NodeCrashExecutor(this, 1);
//            totalExecuted = triggerDiffByLeaderRestart(totalExecuted, client1);

            nodeStartExecutor = new NodeStartExecutor(this, 2);
            nodeCrashExecutor = new NodeCrashExecutor(this, 2);
            totalExecuted = triggerDiffByFollowersRestart(totalExecuted, client1);

            // Ensure client session still in connection!
            totalExecuted = getDataTest(totalExecuted, client1);

            // 2. phantom read
            // Reconfigure executors
            nodeStartExecutor = new NodeStartExecutor(this, 2);
            nodeCrashExecutor = new NodeCrashExecutor(this, 3);
            totalExecuted = followersRestartAndLeaderCrash(totalExecuted);

            int client2 = generateClientId();
//            createClient(client2, true, "127.0.0.1:4001");
            establishSession(client2, true, "127.0.0.1:4001");
            totalExecuted = getDataTest(totalExecuted, client2);

            for (Integer i: clientMap.keySet()) {
                LOG.debug("shutting down client {}", i);
                clientMap.get(i).shutdown();
            }
            clientMap.clear();
            ensemble.stopEnsemble();

            executionWriter.close();
            statisticsWriter.close();
        }
        LOG.debug("total time: {}" , (System.currentTimeMillis() - startTime));
    }

    /***
     * The core process the scheduler by external model
     * @throws SchedulerConfigurationException
     * @throws IOException
     */
    public void startWithExternalModel() throws SchedulerConfigurationException, IOException {
        LOG.debug("Starting the testing service by external model");
        ExternalModelStatistics externalModelStatistics = new ExternalModelStatistics();
        ExternalModelStrategy externalModelStrategy = new ExternalModelStrategy(this,
                new Random(1), schedulerConfiguration.getTraceDir(), externalModelStatistics);
//        bugReportWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
//                + schedulerConfiguration.getBugReportFile());
        matchReportWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                + schedulerConfiguration.getMatchReportFile());

        long startTime = System.currentTimeMillis();
        int traceNum = externalModelStrategy.getTracesNum();
        LOG.debug("Number of traces: {}", traceNum);
        int bugCount = 0;

        for (int executionId = 1; executionId <= traceNum; ++executionId) {
            externalModelStrategy.clearEvents();
            schedulingStrategy = externalModelStrategy;
            statistics = externalModelStatistics;

            Trace trace = externalModelStrategy.getCurrentTrace(executionId - 1);
            String traceName = trace.getTraceName();
            ModelVersion modelVersion = trace.getModelVer();
            int stepCount = trace.getStepCount();
            System.out.println("\n\n\n\n\n");
            LOG.info(">> Replaying Trace {}: {}, model version: {}, total steps: {}",
                    executionId, traceName, modelVersion, stepCount);

            // configure servers
            final int serverNum = trace.getServerNum();
            Map<String, Integer> serverIdMap = new HashMap<>();
            // By default, node mapping in reverse order like:
            // s0 : id = 0
            // s1 : id = 1
            // s2 : id = 2
            for (String serverStr : trace.getServerIds()) {
                serverIdMap.put(serverStr, Integer.parseInt(serverStr.substring(serverStr.length()-1)));
            }
            LOG.debug(">> server num: {}, server mapping: {}", serverNum, serverIdMap);

            // create trace directory, including each server's working directory.
            ensemble.configureEnsemble(traceName, serverNum);

            // create trace execution file
            executionWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getExecutionFile());
            // create trace statistics file
            statisticsWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getStatisticsFile());

            // configure the execution before first election
            configureNextExecution();
            // start the ensemble at the beginning of the execution
            long traceStartTime = System.currentTimeMillis();
            ensemble.startEnsemble();
            executionWriter.write("-----Initialization cost time: " + (System.currentTimeMillis() - traceStartTime) + "\n\n");

            // Start the timer for recoding statistics
            statistics.startTimer();

            int totalExecuted = 0;
            int currentStep = 0;
            String action = "";
            ModelAction modelAction;
            try {
                synchronized (controlMonitor) {
                    waitAllNodesSteady();
                }
                for (; currentStep < stepCount; ++currentStep) {
                    JSONObject jsonObject = trace.getStep(currentStep);
                    Iterator<String> keyIterator = jsonObject.keySet().iterator();
                    String key = keyIterator.next();
                    action = key.equals("Step") ? keyIterator.next() : key;
                    JSONObject elements = jsonObject.getJSONObject(action);

                    // get basic element
                    String serverName = elements.getString("nodeId");
                    int nodeId = serverIdMap.get(serverName);

                    LOG.debug("trace name: {}", traceName);
                    LOG.debug("nextStep: {}", jsonObject);
                    LOG.debug("action: {}, nodeId: {}", action, nodeId);

                    modelAction = ModelAction.valueOf(action);
                    switch (modelAction) {
                        // environmental failures
                        case NodeCrash:
                            totalExecuted = scheduleNodeCrash(nodeId, totalExecuted);

                            // close client session if specified
                            LOG.debug("shutting down client or not: {}", nodeId);
                            boolean closeSession = elements.getBoolean("closeSession")  != null ?
                                    elements.getBoolean("closeSession")  : false;
                            LOG.debug("closeSession: {}", closeSession);
                            if (closeSession) {
                                ClientProxy clientProxy = clientMap.get(nodeId);
                                if (clientProxy != null && !clientProxy.isStop())  {
                                    LOG.debug("shutting down client {}", nodeId);
                                    clientProxy.shutdown();
                                }
                            }
                            break;
                        case NodeStart:
                            totalExecuted = scheduleNodeStart(nodeId, totalExecuted);
                            break;
                        case PartitionStart:
                            int partitionStartPeerId = serverIdMap.get(elements.getString("peerId"));
                            totalExecuted = schedulePartitionStart(nodeId, partitionStartPeerId, totalExecuted);
                            break;
                        case PartitionRecover:
                            int partitionRecoverPeerId = serverIdMap.get(elements.getString("peerId"));
                            totalExecuted = schedulePartitionStop(nodeId, partitionRecoverPeerId, totalExecuted);
                            break;

                        // Client requests. Write requests is modelled in Action: LeaderProcessRequest.
                        case ClientGetData:
                            int getDataClientId = elements.getInteger("clientId") != null ?
                                    elements.getInteger("clientId") : nodeId;
                            boolean shutdown = elements.getBoolean("closeSession")  != null ?
                                    elements.getBoolean("closeSession")  : false;
                            LOG.debug("ClientGetData: {}", getDataClientId);
                            long returnedData = getModelZxidFromArrayForm(elements.getJSONArray("zxid"));
                            totalExecuted = scheduleClientGetData(externalModelStrategy,
                                    currentStep, getDataClientId, nodeId, returnedData,
                                    shutdown, totalExecuted);
                            break;

                        // internal events
                        case SetInitState:
                            JSONArray initStateParticipants = elements.getJSONArray("peerId");
                            List<Integer> initStateParticipantList = initStateParticipants.stream()
                                    .map(p -> serverIdMap.get(p.toString())).collect(Collectors.toList());
                            LOG.debug("SetInitState: election leader: {}, other participants: {}", nodeId, initStateParticipantList);
                            totalExecuted = setInitState(externalModelStrategy, currentStep,
                                    nodeId, initStateParticipantList, serverName, elements, totalExecuted);
                            break;
                        // TODO:
//                        case FLENotmsgTimeout:
//                        case FLEHandleNotmsg:
//                        case FLEReceiveNotmsg:
//                        case FLEWaitNewNotmsg:
//                        case Election:
                        case ElectionAndDiscovery:
                            JSONArray participants = elements.getJSONArray("peerId");
                            List<Integer> peers = participants.stream()
                                    .map(p -> serverIdMap.get(p.toString())).collect(Collectors.toList());
                            // get leader's accepted / new current epoch
                            long leaderAcceptedEpoch = getModelAcceptedEpoch(elements, serverName);
                            long leaderCurrentEpoch = elements.getJSONArray("coreParam").getLong(2);
                            LOG.debug("leaderAcceptedEpoch: {}, leaderCurrentEpoch: {}",
                                    leaderAcceptedEpoch, leaderCurrentEpoch);
                            assert leaderAcceptedEpoch == leaderCurrentEpoch;
                            List<Integer> looking = null;
                            if (elements.getJSONArray("looking") != null) {
                                looking = elements.getJSONArray("looking").stream()
                                        .map(p -> serverIdMap.get(p.toString())).collect(Collectors.toList());
                            }
                            LOG.debug("election leader: {}, other participants: {}, looking: {}",
                                    nodeId, peers, looking);
                            totalExecuted = scheduleElectionAndDiscovery(externalModelStrategy,
                                    currentStep, nodeId, peers, looking, leaderCurrentEpoch, totalExecuted);
                            break;

                        // SYNC-phase actions
                        case LeaderSyncFollower: // send DIFF / TRUNC / SNAP
                            int syncFollowerId = serverIdMap.get(elements.getString("peerId"));
                            LOG.debug("LeaderSyncFollower, {}, {}", nodeId, syncFollowerId);
                            totalExecuted = scheduleLeaderSyncFollower(externalModelStrategy,
                                    nodeId, syncFollowerId, -1L, totalExecuted);
                            break;
                        case FollowerProcessSyncMessage:
                            int syncLeaderId = serverIdMap.get(elements.getString("peerId"));
                            String syncType = elements.getJSONArray("coreParam").getString(0);
                            ModelAction syncAction = getSyncAction(syncType);
                            long syncZxid = getModelZxidFromArrayForm(elements.getJSONArray("coreParam").getJSONArray(1));
                            LOG.debug("{}, syncType: {}, modelZxid: {}", syncAction, syncType, Long.toHexString(syncZxid));
                            pendingProposalsInSync.get(nodeId).clear();
                            pendingCommitsInSync.get(nodeId).clear();
                            int retry = 10;
                            totalExecuted = scheduleInternalEventWithWaitingRetry(externalModelStrategy,
                                    syncAction, nodeId, syncLeaderId, syncZxid, totalExecuted, retry);
                            break;
                        case FollowerProcessPROPOSALInSync:
                            int proposalInSyncLeaderId = serverIdMap.get(elements.getString("peerId"));
                            long lastNotCommitted = getLastNotCommittedInNodePacketsSync(elements, serverName);
                            long lastNotCommittedInParam =
                                    getModelZxidFromArrayForm(elements.getJSONArray("coreParam").getJSONArray(0));
                            LOG.debug("FollowerProcessPROPOSALInSync, lastNotCommitted: {}, lastNotCommittedInParam: {}",
                                    Long.toHexString(lastNotCommitted), Long.toHexString(lastNotCommittedInParam));
                            assert lastNotCommitted == lastNotCommittedInParam;
                            pendingProposalsInSync.get(nodeId).add(lastNotCommitted);
                            totalExecuted = scheduleInternalEventWithWaitingRetry(externalModelStrategy,
                                    modelAction, nodeId, proposalInSyncLeaderId, lastNotCommitted, totalExecuted, 5);
                            break;
                        case FollowerProcessCOMMITInSync:
                            int commitInSyncLeaderId = serverIdMap.get(elements.getString("peerId"));
                            long lastCommitted = getLastCommittedInNodePacketsSync(elements, serverName);
                            long lastCommittedInParam =
                                    getModelZxidFromArrayForm(elements.getJSONArray("coreParam").getJSONArray(0));
                            LOG.debug("FollowerProcessCOMMITInSync, lastCommitted: {}, lastCommittedInParam: {}",
                                    Long.toHexString(lastCommitted), Long.toHexString(lastCommittedInParam));
                            assert lastCommitted == lastCommittedInParam;
                            pendingCommitsInSync.get(nodeId).add(lastCommitted);
                            totalExecuted = scheduleInternalEventWithWaitingRetry(externalModelStrategy,
                                    modelAction, nodeId, commitInSyncLeaderId, lastCommitted, totalExecuted, 5);
                            break;
                        case FollowerProcessNEWLEADER:
                            // distinguish model versions
                            totalExecuted = scheduleFollowerProcessNEWLEADER(externalModelStrategy, modelVersion,
                                    nodeId, serverIdMap.get(elements.getString("peerId")),
                                    -1L, totalExecuted);
                            break;
                        case FollowerProcessNEWLEADERAfterCurrentEpochUpdated:
                            LOG.debug("After FollowerUpdatedCurrentEpoch: {}", nodeId);
                            totalExecuted = scheduleFollowerProcessNEWLEADERAfterCurrentEpochUpdated(externalModelStrategy,
                                    modelVersion, nodeId, -1, -1, totalExecuted);
                            break;
                        case LeaderProcessACKLD: // release ACK && release learner handler's readRecord
                            int ackldFollowerId = serverIdMap.get(elements.getString("peerId"));
                            LOG.debug("{}, modelZxid: {}", action, -1L);
                            totalExecuted = scheduleLeaderProcessACKLD(externalModelStrategy,
                                    modelAction, nodeId, ackldFollowerId, -1L, totalExecuted);
                            break;
                        case FollowerProcessUPTODATE: // release UPTODATE
                            int uptodateLeaderId = serverIdMap.get(elements.getString("peerId"));
                            LOG.debug("{}, modelZxid: {}", action, 0L);
                            totalExecuted = scheduleFollowerProcessUPTODATE(externalModelStrategy,
                                    modelVersion, nodeId, uptodateLeaderId, 0L, totalExecuted);
                            break;

                        // BROADCAST-phase actions
                        case LeaderProcessRequest:
                            // release set data & release leader log
                            int setDataClientId = elements.getInteger("clientId") != null ?
                                    elements.getInteger("clientId") : nodeId;
                            long lastZxidInLeaderHistory = getLastZxidInNodeHistory(elements, serverName);
                            long lastWrittenZxid = getModelZxidFromArrayForm(elements.getJSONArray("coreParam").getJSONArray(0));
                            assert lastWrittenZxid == lastZxidInLeaderHistory;
                            LOG.debug("LeaderProcessRequest modelZxid: {}", Long.toHexString(lastZxidInLeaderHistory));
                            totalExecuted = scheduleLeaderProcessRequest(externalModelStrategy,
                                    setDataClientId, nodeId, lastZxidInLeaderHistory, totalExecuted);
                            break;
                        case FollowerProcessPROPOSAL:
                            // distinguish model versions
                            int proposalLeaderId = serverIdMap.get(elements.getString("peerId"));
                            long lastZxidInParam =
                                    getModelZxidFromArrayForm(elements.getJSONArray("coreParam").getJSONArray(0));
                            LOG.debug("FollowerProcessPROPOSAL, lastZxidInParam: {}", Long.toHexString(lastZxidInParam));
                            totalExecuted = scheduleFollowerProcessPROPOSAL(externalModelStrategy, modelVersion,
                                    nodeId, proposalLeaderId, lastZxidInParam, totalExecuted);
                            break;
                        case FollowerSyncProcessorLogRequest:
                            long followerLastZxidInHistory = getLastZxidInNodeHistory(elements, serverName);
                            long LoggedZxidInParam =
                                    getModelZxidFromArrayForm(elements.getJSONArray("coreParam").getJSONArray(0));
                            LOG.debug("FollowerSyncProcessorLogRequest, followerLastZxidInHistory: {}, LoggedZxidInParam: {}",
                                    Long.toHexString(followerLastZxidInHistory), Long.toHexString(LoggedZxidInParam));
                            assert followerLastZxidInHistory == LoggedZxidInParam;
                            totalExecuted = scheduleInternalEventWithWaitingRetry(externalModelStrategy, ModelAction.FollowerLog,
                                    nodeId, -1, followerLastZxidInHistory, totalExecuted, 3);
                            break;
                        case LeaderProcessACK:
                            // release ACK && release learner handler's readRecord && release COMMIT
                            String followerNode = elements.getString("peerId");
                            int ackFollowerId = serverIdMap.get(followerNode);
                            long followerLogged =
                                    getModelZxidFromArrayForm(elements.getJSONArray("coreParam").getJSONArray(0));
                            LOG.debug("LeaderProcessACK, leader: {}, follower {}, ack-zxid: {}",
                                    nodeId, ackFollowerId, Long.toHexString(followerLogged));
                            totalExecuted = scheduleLeaderProcessACK(externalModelStrategy,
                                    nodeId, ackFollowerId, followerLogged, totalExecuted);
                            break;
                        case FollowerProcessCOMMIT:
                            // release COMMIT
                            // distinguish model versions
                            int commitLeaderId = serverIdMap.get(elements.getString("peerId"));
                            long committedInParam =
                                    getModelZxidFromArrayForm(elements.getJSONArray("coreParam").getJSONArray(0));
                            LOG.debug("FollowerProcessCOMMIT, committedInParam: {}", Long.toHexString(committedInParam));
                            totalExecuted = scheduleFollowerProcessCOMMIT(externalModelStrategy, modelVersion,
                                    nodeId, commitLeaderId, committedInParam, totalExecuted);
                            break;
                        case FollowerCommitProcessorCommit:
                            long followerLastCommitted = getLastZxidInNodeLastCommitted(elements, serverName);
                            long CommittedZxidInParam =
                                    getModelZxidFromArrayForm(elements.getJSONArray("coreParam").getJSONArray(0));
                            LOG.debug("FollowerCommitProcessorCommit, followerLastCommitted: {}, CommittedZxidInParam: {}",
                                    Long.toHexString(followerLastCommitted), Long.toHexString(CommittedZxidInParam));
                            assert followerLastCommitted == CommittedZxidInParam;
                            totalExecuted = scheduleInternalEventWithWaitingRetry(externalModelStrategy, ModelAction.FollowerCommit,
                                    nodeId, -1, followerLastCommitted, totalExecuted, 3);
                            break;

                        default:
                            LOG.debug("No matching event for model action {}", modelAction);
                            throw new SchedulerConfigurationException();
                    }

                    if (!committedLogVerifier.verify()) {
                        bugReportWriter.write("Trace: " + traceName + "\n");
                        bugReportWriter.write(statistics.toString() + "\n\n");
                    }
                    statistics.reportCurrentStep("[Step " + (currentStep + 1) + "/" + stepCount + "]-" + action);
                    statistics.reportTotalExecutedEvents(totalExecuted);
                    statisticsWriter.write(statistics.toString() + "\n\n");
                    LOG.info("trace: {}", traceName);
                    LOG.info(statistics.toString() + "\n\n\n\n\n");

                }
            } catch (SchedulerConfigurationException e) {
                LOG.info("SchedulerConfigurationException found when scheduling Trace {} in Step {} / {}. ",
                        traceName, currentStep, stepCount);
                e.printStackTrace();
                tracePassed = false;
            } catch (NullPointerException e) {
                LOG.info("NullPointerException found when scheduling Trace {} in Step {} / {}. ",
                        traceName, currentStep, stepCount);
                e.printStackTrace();
                tracePassed = false;
            } catch (IllegalArgumentException e) {
                LOG.info("The scheduler cannot match action {}. " +
                                "IllegalArgumentException found when scheduling Trace {} in Step {} / {}. ",
                        action, traceName, currentStep, stepCount);
                e.printStackTrace();
                tracePassed = false;
            } finally {
                statistics.endTimer();

                // report statistics of total trace
                LOG.info("tracename: {}, setTraceLen: {}, setExecutedStep: {} ", traceName, stepCount, currentStep);
                traceVerifier.setTraceLen(stepCount);
                traceVerifier.setExecutedStep(currentStep);
                boolean matchedAndPassed = traceVerifier.verify();
                String info = currentStep >= stepCount ? "COMPLETE" : action;
                statistics.reportCurrentStep("[Step " + currentStep + "/" + stepCount + "]-" + info);
                statistics.reportTotalExecutedEvents(totalExecuted);
                statisticsWriter.write(statistics.toString() + "\n\n");
                LOG.info(statistics.toString() + "\n\n\n\n\n");

                executionWriter.write("\ntrace time/ms: " + (System.currentTimeMillis() - traceStartTime) + "\n");
                executionWriter.flush();

//                if (!matchedAndPassed) {
//                    bugCount++;
//                    bugReportWriter.write("Trace: " + traceName + "\n");
//                    bugReportWriter.write(statistics.toString() + "\n\n");
//                }
                if (!traceMatched) {
                    matchReportWriter.write("Trace: " + traceName + "\n");
                    matchReportWriter.write(statistics.toString() + "\n\n");
                }

                // shutdown clients & servers
                synchronized (controlMonitor) {
                    for (Integer i: clientMap.keySet()) {
                        LOG.debug("shutting down client {}", i);
                        clientMap.get(i).shutdown();
                        controlMonitor.notifyAll();
                        waitClientSessionClosed(i);
                    }
                }
                clientMap.clear();
                ensemble.stopEnsemble();

                synchronized (controlMonitor) {
                    while (schedulingStrategy.hasNextEvent()) {
                        Event event = schedulingStrategy.nextEvent();
                        event.setFlag(TestingDef.RetCode.EXIT);
                        event.execute();
                    }
                    controlMonitor.notifyAll();
                    waitAllNodesSteady();
                }

                executionWriter.close();
                statisticsWriter.close();
//                bugReportWriter.flush();
                matchReportWriter.flush();
            }
        }
        LOG.debug("total time: {}" , (System.currentTimeMillis() - startTime));
        final int unmatchedCount = TraceVerifier.getUnmatchedCount();
        final int failedCount = TraceVerifier.getFailedCount();
        final float unmatchedRate = (float) unmatchedCount / traceNum ;
//        final float failedRate = (float) failedCount / traceNum ;
//        final float bugRate = (float) bugCount / traceNum ;
        matchReportWriter.write("TOTAL: " + traceNum + "\n");
//        bugReportWriter.write("BUG:\t" + bugCount + "\tNO_BUG:\t" + (traceNum - bugCount) +
//                "\tBUG RATE:\t" + bugRate + "\n");
//        bugReportWriter.write("UNMATCH:\t" + unmatchedCount + "\tMATCH:\t" + (traceNum - unmatchedCount) +
//                "\tUNMATCHED RATE:\t" + unmatchedRate + "\n");
//        bugReportWriter.write("FAIL:\t" + failedCount + "\tPASS:\t" + (traceNum - failedCount) +
//                "\tFAIL RATE:\t" + failedRate + "\n");
//        bugReportWriter.close();
        matchReportWriter.write("UNMATCH:\t" + unmatchedCount + "\tMATCH:\t" + (traceNum - unmatchedCount) +
                "\tUNMATCHED RATE:\t" + unmatchedRate + "\n");
        matchReportWriter.close();
    }


    public void startWithExternalModel2() throws SchedulerConfigurationException, IOException {
        LOG.debug("Starting the testing service by external model");
        ExternalModelStatistics externalModelStatistics = new ExternalModelStatistics();
        ExternalModelStrategy externalModelStrategy = new ExternalModelStrategy(this,
                new Random(1), schedulerConfiguration.getTraceDir(), externalModelStatistics);
//        bugReportWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
//                + schedulerConfiguration.getBugReportFile());
        matchReportWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                + schedulerConfiguration.getMatchReportFile());

        long startTime = System.currentTimeMillis();
        int traceNum = externalModelStrategy.getTracesNum();
        LOG.debug("Number of traces: {}", traceNum);
        int bugCount = 0;

        for (int executionId = 1; executionId <= traceNum; ++executionId) {
            externalModelStrategy.clearEvents();
            schedulingStrategy = externalModelStrategy;
            statistics = externalModelStatistics;

            Trace trace = externalModelStrategy.getCurrentTrace(executionId - 1);
            String traceName = trace.getTraceName();
            ModelVersion modelVersion = trace.getModelVer();
            int stepCount = trace.getStepCount();
            System.out.println("\n\n\n\n\n");
            LOG.info(">> Replaying Trace {}: {}, model version: {}, total steps: {}",
                    executionId, traceName, modelVersion, stepCount);

            // configure servers
            final int serverNum = trace.getServerNum();
            Map<String, Integer> serverIdMap = new HashMap<>();
            // By default, node mapping in reverse order like:
            // s0 : id = 0
            // s1 : id = 1
            // s2 : id = 2
            for (String serverStr : trace.getServerIds()) {
                LOG.debug(">> serverStr: {}, server mapping: {}", serverStr, serverStr.substring(serverStr.length()-1));
                serverIdMap.put(serverStr, Integer.parseInt(serverStr.substring(serverStr.length()-1)));
            }
            LOG.debug(">> server number: {}, server mapping: {}", serverNum, serverIdMap);

            // create trace directory, including each server's working directory.
            ensemble.configureEnsemble(traceName, serverNum);

            // create trace execution file
            executionWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getExecutionFile());
            // create trace statistics file
            statisticsWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getStatisticsFile());

            // configure the execution before first election
            configureNextExecution();
            // start the ensemble at the beginning of the execution
            long traceStartTime = System.currentTimeMillis();
            ensemble.startEnsemble();
            executionWriter.write("-----Initialization cost time: " + (System.currentTimeMillis() - traceStartTime) + "\n\n");

            // Start the timer for recoding statistics
            statistics.startTimer();

            int totalExecuted = 0;
            int currentStep = 0;
            String action = "";
            ModelAction modelAction;
            try {
                synchronized (controlMonitor) {
                    waitAllNodesSteady();
                }

                totalExecuted = scheduleTargetEvents(externalModelStrategy, currentStep, totalExecuted);

                if (!committedLogVerifier.verify()) {
                    bugReportWriter.write("Trace: " + traceName + "\n");
                    bugReportWriter.write(statistics.toString() + "\n\n");
                }
                statistics.reportCurrentStep("[Step " + totalExecuted + "/" + stepCount + "]-" + action);
                statistics.reportTotalExecutedEvents(totalExecuted);
                statisticsWriter.write(statistics.toString() + "\n\n");
                LOG.info("trace: {}", traceName);
                LOG.info(statistics.toString() + "\n\n\n\n\n");

            } catch (SchedulerConfigurationException e) {
                LOG.info("SchedulerConfigurationException found when scheduling Trace {} in Step {} / {}. ",
                        traceName, currentStep, stepCount);
                e.printStackTrace();
                tracePassed = false;
            } catch (NullPointerException e) {
                LOG.info("NullPointerException found when scheduling Trace {} in Step {} / {}. ",
                        traceName, currentStep, stepCount);
                e.printStackTrace();
                tracePassed = false;
            } catch (IllegalArgumentException e) {
                LOG.info("The scheduler cannot match action {}. " +
                                "IllegalArgumentException found when scheduling Trace {} in Step {} / {}. ",
                        action, traceName, currentStep, stepCount);
                e.printStackTrace();
                tracePassed = false;
            } finally {
                statistics.endTimer();

                // report statistics of total trace
                LOG.info("tracename: {}, setTraceLen: {}, setExecutedStep: {} ", traceName, stepCount, currentStep);
                traceVerifier.setTraceLen(stepCount);
                traceVerifier.setExecutedStep(currentStep);
                boolean matchedAndPassed = traceVerifier.verify();
                String info = currentStep >= stepCount ? "COMPLETE" : action;
                statistics.reportCurrentStep("[Step " + currentStep + "/" + stepCount + "]-" + info);
                statistics.reportTotalExecutedEvents(totalExecuted);
                statisticsWriter.write(statistics.toString() + "\n\n");
                LOG.info(statistics.toString() + "\n\n\n\n\n");

                executionWriter.write("\ntrace time/ms: " + (System.currentTimeMillis() - traceStartTime) + "\n");
                executionWriter.flush();

//                if (!matchedAndPassed) {
//                    bugCount++;
//                    bugReportWriter.write("Trace: " + traceName + "\n");
//                    bugReportWriter.write(statistics.toString() + "\n\n");
//                }
                if (!traceMatched) {
                    matchReportWriter.write("Trace: " + traceName + "\n");
                    matchReportWriter.write(statistics.toString() + "\n\n");
                }

                // shutdown clients & servers
                synchronized (controlMonitor) {
                    for (Integer i: clientMap.keySet()) {
                        LOG.debug("shutting down client {}", i);
                        clientMap.get(i).shutdown();
                        controlMonitor.notifyAll();
                        waitClientSessionClosed(i);
                    }
                }
                clientMap.clear();
                ensemble.stopEnsemble();

                synchronized (controlMonitor) {
                    while (schedulingStrategy.hasNextEvent()) {
                        Event event = schedulingStrategy.nextEvent();
                        event.setFlag(TestingDef.RetCode.EXIT);
                        event.execute();
                    }
                    controlMonitor.notifyAll();
                    waitAllNodesSteady();
                }

                executionWriter.close();
                statisticsWriter.close();
//                bugReportWriter.flush();
                matchReportWriter.flush();
            }
        }
        LOG.debug("total time: {}" , (System.currentTimeMillis() - startTime));
        final int unmatchedCount = TraceVerifier.getUnmatchedCount();
        final int failedCount = TraceVerifier.getFailedCount();
        final float unmatchedRate = (float) unmatchedCount / traceNum ;
//        final float failedRate = (float) failedCount / traceNum ;
//        final float bugRate = (float) bugCount / traceNum ;
        matchReportWriter.write("TOTAL: " + traceNum + "\n");
//        bugReportWriter.write("BUG:\t" + bugCount + "\tNO_BUG:\t" + (traceNum - bugCount) +
//                "\tBUG RATE:\t" + bugRate + "\n");
//        bugReportWriter.write("UNMATCH:\t" + unmatchedCount + "\tMATCH:\t" + (traceNum - unmatchedCount) +
//                "\tUNMATCHED RATE:\t" + unmatchedRate + "\n");
//        bugReportWriter.write("FAIL:\t" + failedCount + "\tPASS:\t" + (traceNum - failedCount) +
//                "\tFAIL RATE:\t" + failedRate + "\n");
//        bugReportWriter.close();
        matchReportWriter.write("UNMATCH:\t" + unmatchedCount + "\tMATCH:\t" + (traceNum - unmatchedCount) +
                "\tUNMATCHED RATE:\t" + unmatchedRate + "\n");
        matchReportWriter.close();
    }


    public int scheduleTargetEvents(final ExternalModelStrategy strategy,
                            final Integer currentStep,
                            int totalExecuted) throws SchedulerConfigurationException {


        int leaderId = 2;
        List<Integer> peers = new ArrayList<>(Arrays.asList(0, 1));
        LOG.debug("election leader: {}, other participants: {}",
                leaderId, peers);
        scheduleElectionAndDiscovery(strategy, currentStep, leaderId, peers, null, 1L, totalExecuted);

        long modelZxid = -1L;
        int retry1 = 3;

        for (Integer peer: peers) {
            scheduleLeaderSyncFollower(strategy, leaderId, peer, -1L, totalExecuted);
        }

        // sync
        for (Integer peer: peers) {
            scheduleInternalEventWithWaitingRetry(strategy,
                    ModelAction.FollowerProcessDIFF, peer, leaderId, modelZxid, totalExecuted, retry1);
        }
        for (Integer peer: peers) {
            scheduleFollowerProcessNEWLEADER(strategy, ModelVersion.DEFAULT,
                    peer, leaderId, -1L, totalExecuted);
        }
        for (Integer peer: peers) {
            scheduleFollowerProcessNEWLEADERAfterCurrentEpochUpdated(strategy,
                    ModelVersion.DEFAULT, peer, -1, -1, totalExecuted);
        }
        for (Integer peer: peers) {
            scheduleLeaderProcessACKLD(strategy,
                    ModelAction.LeaderProcessACKLD, leaderId, peer, modelZxid, totalExecuted);
        }
        for (Integer peer: peers) {
            scheduleFollowerProcessUPTODATE(strategy,
                    ModelVersion.DEFAULT, peer, leaderId, 0L, totalExecuted);
        }

        // IN BROADCAST
        // SetData
        totalExecuted = scheduleClientSetData(strategy,
                leaderId, leaderId, -1L, totalExecuted);

        // S0, S1 restart
        totalExecuted = scheduleNodeCrash(0, totalExecuted);
        totalExecuted = scheduleNodeStart(0, totalExecuted);
        totalExecuted = scheduleNodeCrash(1, totalExecuted);
        totalExecuted = scheduleNodeStart(1, totalExecuted);

        // S1 elected
        leaderId = 1;
        peers = new ArrayList<>(Arrays.asList(0));
        totalExecuted = scheduleElectionAndDiscovery(strategy, currentStep, leaderId, peers, null, 2L, totalExecuted);

        // S2 joins the ensemble
        //

        return totalExecuted;

    }

    @Deprecated
    /***
     * The traces are provided by external sequences of events
     * Note: The granularity of the trace is coarse with only events with little simple info
     * @throws SchedulerConfigurationException
     * @throws IOException
     */
    public void startWithEventSequence() throws SchedulerConfigurationException, IOException {
        LOG.debug("Starting the testing service by external model");
        ExternalModelStatistics externalModelStatistics = new ExternalModelStatistics();
        EventSequenceStrategy eventSequenceStrategy = new EventSequenceStrategy(this, new Random(1), schedulerConfiguration.getTraceDir(), externalModelStatistics);

        long startTime = System.currentTimeMillis();
        int traceNum = eventSequenceStrategy.getTracesNum();
        LOG.debug("traceNum: {}", traceNum);

        for (int executionId = 1; executionId <= traceNum; ++executionId) {
            eventSequenceStrategy.clearEvents();
            schedulingStrategy = eventSequenceStrategy;
            statistics = externalModelStatistics;

            EventSequence trace = eventSequenceStrategy.getCurrentTrace(executionId - 1);
            String traceName = trace.getTraceName();
            int stepCount = trace.getStepNum();
            LOG.info("currentTrace: {}, total steps: {}", trace, stepCount);

            ensemble.configureEnsemble(traceName);

            int totalExecuted = 0;

            executionWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getExecutionFile());
            statisticsWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getStatisticsFile());

            // configure the execution before first election
            configureNextExecution();
            // start the ensemble at the beginning of the execution
            long traceStartTime = System.currentTimeMillis();
            ensemble.startEnsemble();
            executionWriter.write("-----Initialization cost time: " + (System.currentTimeMillis() - traceStartTime) + "\n\n");

            // Start the timer for recoding statistics
            statistics.startTimer();

            int currentStep = 1;
            String line = null;
            try {
                for (; currentStep <= stepCount; ++currentStep) {
                    line = trace.nextStep();
                    LOG.debug("nextStep: {}", line);

                    String[] lineArr = line.split(" ");
                    int len = lineArr.length;

                    String action = lineArr[0];
                    switch (action) {
                        case "ELECTION":
                            Integer leaderId = len > 1 ? Integer.parseInt(lineArr[1]) : null;
                            LOG.debug("election leader: {}", leaderId);
                            totalExecuted = scheduleElection(currentStep, line, leaderId, totalExecuted);
                            break;
//                    case "LOG_REQUEST":
//                        assert len>=2;
//                        int logNodeId = Integer.parseInt(lineArr[1]);
//                        totalExecuted = scheduleLogRequest(logNodeId, totalExecuted);
//                        break;
//                    case "COMMIT":
//                        assert len>=2;
//                        int commitNodeId = Integer.parseInt(lineArr[1]);
//                        totalExecuted = scheduleCommit(commitNodeId, totalExecuted);
//                        break;
//                    case "SEND_PROPOSAL":
//                    case "SEND_COMMIT":
//                        assert len>=3;
//                        int s1 = Integer.parseInt(lineArr[1]);
//                        int s2 = Integer.parseInt(lineArr[2]);
//                        totalExecuted = scheduleLearnerHandlerMessage(s1, s2, totalExecuted);
//                        break;
                        case "LOG_REQUEST":
                        case "COMMIT":
                        case "SEND_PROPOSAL":
                        case "SEND_COMMIT":
                            totalExecuted = scheduleInternalEventInSequence(eventSequenceStrategy, lineArr, totalExecuted);
                            break;
                        case "NODE_CRASH":
                            assert len==2;
                            int crashNodeId = Integer.parseInt(lineArr[1]);
                            totalExecuted = scheduleNodeCrash(crashNodeId, totalExecuted);
                            break;
                        case "NODE_START":
                            assert len==2;
                            int startNodeId = Integer.parseInt(lineArr[1]);
                            totalExecuted = scheduleNodeStart(startNodeId, totalExecuted);
                            break;
                        case "PARTITION_START":
                            assert len==3;
                            int node1 = Integer.parseInt(lineArr[1]);
                            int node2 = Integer.parseInt(lineArr[2]);
                            LOG.debug("----PARTITION_START: {}, {}", node1, node2);
                            totalExecuted = schedulePartitionStart(node1, node2, totalExecuted);
                            break;
                        case "PARTITION_STOP":
                            assert len==3;
                            int node3 = Integer.parseInt(lineArr[1]);
                            int node4 = Integer.parseInt(lineArr[2]);
                            LOG.debug("----PARTITION_STOP: {}, {}", node3, node4);
                            totalExecuted = schedulePartitionStop(node3, node4, totalExecuted);
                            break;
                        case "ESTABLISH_SESSION":
                            assert len==3;
                            int clientId = Integer.parseInt(lineArr[1]);
                            int serverId = Integer.parseInt(lineArr[2]);
                            String serverAddr = getServerAddr(serverId);
                            LOG.debug("client establish connection with server {}", serverAddr);
                            establishSession(clientId, true, serverAddr);
                            break;
                        case "GET_DATA":
                            assert len>=3;
                            int getDataClientId = Integer.parseInt(lineArr[1]);
                            int sid = Integer.parseInt(lineArr[2]);
                            Integer result = len > 3 ? Integer.parseInt(lineArr[3]) : null;
                            totalExecuted = scheduleGetData(currentStep, line, getDataClientId, sid, result, totalExecuted);
                            break;
                        case "SET_DATA":
                            assert len>=3;
                            int setDataClientId = Integer.parseInt(lineArr[1]);
                            int sid2 = Integer.parseInt(lineArr[2]);
                            String data = len > 3 ? lineArr[3] : null;
                            totalExecuted = scheduleSetData(setDataClientId, sid2, data, totalExecuted);
                            break;
                    }
                }
            } catch (SchedulerConfigurationException e) {
                LOG.info("SchedulerConfigurationException found when scheduling EventSequence {} in Step {} / {}. " +
                        "Complete trace: {}", traceName, currentStep, stepCount, trace);
                tracePassed = false;
            } finally {
                statistics.endTimer();

                // report statistics of total trace
                traceVerifier.setTraceLen(stepCount);
                traceVerifier.setExecutedStep(currentStep);
                traceVerifier.verify();
                if (currentStep > stepCount) line = "COMPLETE";
                statistics.reportCurrentStep("[LINE " + currentStep + "]-" + line);
                statistics.reportTotalExecutedEvents(totalExecuted);
                statisticsWriter.write(statistics.toString() + "\n\n");
                LOG.info(statistics.toString() + "\n\n\n\n\n");

                // shutdown clients & servers
                for (Integer i: clientMap.keySet()) {
                    LOG.debug("shutting down client {}", i);
                    clientMap.get(i).shutdown();
                }
                clientMap.clear();
                ensemble.stopEnsemble();

                executionWriter.close();
                statisticsWriter.close();
            }
        }
        LOG.debug("total time: {}" , (System.currentTimeMillis() - startTime));

    }

    private ModelAction getSyncAction(String syncType) throws SchedulerConfigurationException {
        switch (syncType) {
            case "DIFF":
                return ModelAction.FollowerProcessDIFF;
            case "TRUNC":
                return ModelAction.FollowerProcessTRUNC;
            case "SNAP":
                return ModelAction.FollowerProcessSNAP;
            default:
                throw new SchedulerConfigurationException();
        }
    }

    private static long makeZxid(long epoch, long counter) {
        return (epoch << 32L) | (counter & 0xffffffffL);
    }

    private long getModelZxidFromArrayForm(final JSONArray modelZxidArrayForm) {
        long epoch = modelZxidArrayForm.getLong(0) ;
        long counter = modelZxidArrayForm.getLong(1) ;
        return makeZxid(epoch, counter);
    }
    /***
     * For action : LeaderProcessRequest & FollowerProcessPROPOSAL & LeaderProcessACK
     * variables -> history
     * @param elements
     * @param serverName
     * @return
     */
    private long getLastZxidInNodeHistory(JSONObject elements, String serverName) {
        try {
            JSONArray serverHistory = elements.getJSONObject("variables").getJSONObject("history").getJSONArray(serverName);
            JSONArray modelZxidArrayForm = serverHistory.getJSONObject(serverHistory.size() - 1).getJSONArray("zxid");
            return getModelZxidFromArrayForm(modelZxidArrayForm);
        } catch (NullPointerException e) {
            LOG.debug("NullPointerException found when getLastNotCommittedInNodePacketsSync in {}", elements);
            return -1L;
        }
    }

    /***
     * For action : FollowerProcessCOMMIT
     * variables -> lastCommitted
     * @param elements
     * @param serverName
     * @return
     */
    private long getLastZxidInNodeLastCommitted(JSONObject elements, String serverName) {
        try {
            JSONObject lastCommittted = elements.getJSONObject("variables").getJSONObject("lastCommitted").getJSONObject(serverName);
            JSONArray modelZxidArrayForm = lastCommittted.getJSONArray("zxid");
            return getModelZxidFromArrayForm(modelZxidArrayForm);
        } catch (NullPointerException e) {
            LOG.debug("NullPointerException found when getLastNotCommittedInNodePacketsSync in {}", elements);
            return -1L;
        }
    }

    /***
     * For action : FollowerProcessPROPOSALInSync:
     * variables -> packetsSync -> notCommitted
     * @param elements
     * @param serverName
     * @return
     */
    private long getLastNotCommittedInNodePacketsSync(JSONObject elements, String serverName) {
        try {
            JSONArray notCommitted = elements.getJSONObject("variables").getJSONObject("packetsSync")
                    .getJSONObject(serverName).getJSONArray("notCommitted");
            if (notCommitted.size() == 0) {
                return -1L;
            }
            JSONArray modelZxidArrayForm = notCommitted.getJSONObject(notCommitted.size()-1).getJSONArray("zxid");
            return getModelZxidFromArrayForm(modelZxidArrayForm);
        } catch (NullPointerException e) {
            LOG.debug("NullPointerException found when getLastNotCommittedInNodePacketsSync in {}", elements);
            return -1L;
        }

    }

    /***
     * For action : FollowerProcessCOMMITInSync:
     * variables -> packetsSync -> committed
     * @param elements
     * @param serverName
     * @return
     */
    private long getLastCommittedInNodePacketsSync(JSONObject elements, String serverName) {
        try {
            JSONArray committed = elements.getJSONObject("variables").getJSONObject("packetsSync")
                    .getJSONObject(serverName).getJSONArray("committed");
            if (committed.size() == 0) {
                return -1L;
            }
            JSONArray modelZxidArrayForm = committed.getJSONArray(committed.size()-1);
            return getModelZxidFromArrayForm(modelZxidArrayForm);
        }  catch (NullPointerException e) {
            LOG.debug("NullPointerException found when getLastNotCommittedInNodePacketsSync in {}", elements);
            return -1L;
        }
    }

    private long getModelAcceptedEpoch(JSONObject elements, String serverName) {
        try {
            return elements.getJSONObject("variables").getJSONObject("acceptedEpoch").getLong(serverName);
        } catch (NullPointerException e) {
            LOG.debug("NullPointerException found when getModelAcceptedEpoch in {}", elements);
            return -1L;
        }
    }

    public String getServerAddr(int serverId) {
        return "localhost:" + (schedulerConfiguration.getClientPort() + serverId);
    }

    public void initRemote() {
        try {
            final TestingRemoteService testingRemoteService = (TestingRemoteService) UnicastRemoteObject.exportObject(this, 0);
            final Registry registry = LocateRegistry.createRegistry(2599);
//            final Registry registry = LocateRegistry.getRegistry(2599);
            System.out.println("\n\n\n\n\n");
            LOG.debug("{}, {}", TestingRemoteService.REMOTE_NAME, testingRemoteService);
            registry.rebind(TestingRemoteService.REMOTE_NAME, testingRemoteService);
            LOG.debug("Bound the remote testing service. ");
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception while initializing the scheduler.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getMessageInFlight() {
        return messageInFlight;
    }

    @Override
    public int getLogRequestInFlight() {
        return logRequestInFlight;
    }

    public void setLastNodeStartEvent(final int nodeId, final NodeStartEvent nodeStartEvent) {
        lastNodeStartEvents.set(nodeId, nodeStartEvent);
    }

    /***
     * Configure scheduling strategy before each execution
     * @param executionId
     */
    private void configureSchedulingStrategy(final int executionId) {
        // Configure scheduling strategy
        final Random random = new Random();
        final long seed;
        if (schedulerConfiguration.hasRandomSeed()) {
            seed = schedulerConfiguration.getRandomSeed() + executionId * executionId;
        }
        else {
            seed = random.nextLong();
        }
        random.setSeed(seed);

        if ("pctcp".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final PCTCPStatistics pctcpStatistics = new PCTCPStatistics();
            statistics = pctcpStatistics;

            LOG.debug("Configuring PCTCPStrategy: maxEvents={}, numPriorityChangePoints={}, randomSeed={}",
                    schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), seed);
            pctcpStatistics.reportMaxEvents(schedulerConfiguration.getMaxEvents());
            pctcpStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            schedulingStrategy = new PCTCPStrategy(schedulerConfiguration.getMaxEvents(),
                    schedulerConfiguration.getNumPriorityChangePoints(), random, pctcpStatistics);
        }
        else if ("tapct".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final TAPCTstatistics tapctStatistics = new TAPCTstatistics();
            statistics = tapctStatistics;

            LOG.debug("Configuring taPCTstrategy: maxEvents={}, numPriorityChangePoints={}, randomSeed={}",
                    schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), seed);
            tapctStatistics.reportMaxEvents(schedulerConfiguration.getMaxEvents());
            tapctStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            schedulingStrategy = new TAPCTstrategy(
                    schedulerConfiguration.getMaxEvents(),
                    schedulerConfiguration.getNumPriorityChangePoints(), random, tapctStatistics);
        }
        else if ("Poffline".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final PofflineStatistics pOfflineStatistics = new PofflineStatistics();
            statistics = pOfflineStatistics;

            LOG.debug("Configuring PofflineStrategy: maxEvents={}, numPriorityChangePoints={}, randomSeed={}",
                    schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), seed);
            pOfflineStatistics.reportMaxEvents(schedulerConfiguration.getMaxEvents());
            pOfflineStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            schedulingStrategy = new PofflineStrategy(schedulerConfiguration.getMaxEvents(),
                    schedulerConfiguration.getNumPriorityChangePoints(), random, pOfflineStatistics);
        }
        else if("pos".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final POSstatistics posStatistics = new POSstatistics();
            statistics = posStatistics;

            LOG.debug("Configuring POSstrategy: randomSeed={}", seed);
            schedulingStrategy = new POSstrategy(schedulerConfiguration.getMaxEvents(), random, posStatistics);
        }

        else if("posd".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final POSdStatistics posdStatistics = new POSdStatistics();
            statistics = posdStatistics;

            posdStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            LOG.debug("Configuring POSdStrategy: randomSeed={}", seed);
            schedulingStrategy = new POSdStrategy(schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), random, posdStatistics);
        }
        else if("rapos".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final RAPOSstatistics raposStatistics = new RAPOSstatistics();
            statistics = raposStatistics;

            LOG.debug("Configuring RAPOSstrategy: randomSeed={}", seed);
            schedulingStrategy = new RAPOSstrategy(random, raposStatistics);
        }
        else {
            final RandomWalkStatistics randomWalkStatistics = new RandomWalkStatistics();
            statistics = randomWalkStatistics;

            LOG.debug("Configuring RandomWalkStrategy: randomSeed={}", seed);
            schedulingStrategy = new RandomWalkStrategy(random, randomWalkStatistics);
        }
        statistics.reportRandomSeed(seed);
    }

    /***
     * Configure all testing metadata before each execution
     */
    private void configureNextExecution() {

        final int nodeNum = schedulerConfiguration.getNumNodes();

        // Configure external event executors
        nodeStartExecutor = new NodeStartExecutor(this, schedulerConfiguration.getNumReboots());
        nodeCrashExecutor = new NodeCrashExecutor(this, schedulerConfiguration.getNumCrashes());

        clientRequestWaitingResponseExecutor = new ClientRequestExecutor(this, true, 0);
        clientRequestExecutor = new ClientRequestExecutor(this, false, 0);

        partitionStartExecutor = new PartitionStartExecutor(this, 10);
        partitionStopExecutor = new PartitionStopExecutor(this, 10);

        // Configure internal event executors
        electionMessageExecutor = new ElectionMessageExecutor(this);
        localEventExecutor = new LocalEventExecutor(this);
        leaderToFollowerMessageExecutor = new LeaderToFollowerMessageExecutor(this);
        followerToLeaderMessageExecutor = new FollowerToLeaderMessageExecutor(this);

        // Configure checkers
        traceVerifier = new TraceVerifier(this, statistics);
        leaderElectionVerifier = new LeaderElectionVerifier(this, statistics);
        getDataVerifier = new GetDataVerifier(this, statistics);
        committedLogVerifier = new CommittedLogVerifier(this, statistics);

        // for property check
        votes.clear();
        votes.addAll(Collections.<Vote>nCopies(nodeNum, null));

        leaderElectionStates.clear();
        leaderElectionStates.addAll(Collections.nCopies(nodeNum, LeaderElectionState.LOOKING));

        returnedDataList.clear();
        returnedDataList.add("0");

        lastCommittedZxid.clear();
        lastCommittedZxid.add(0L);

        lastProcessedZxids.clear();
        acceptedEpochs.clear();
        currentEpochs.clear();
        pendingProposalsInSync.clear();
        pendingCommitsInSync.clear();
        allZxidRecords.clear();
        modelToCodeZxidMap.clear();

        zxidSyncedMap.clear();
        zxidToCommitMap.clear();
        leaderSyncFollowerCountMap.clear();

        syncTypeList.clear();

        traceMatched = true;
        tracePassed = true;

        clientInitializationDone = true;

        // Configure nodes and subnodes

        nodeStates.clear();
        nodePhases.clear();

        subnodeSets.clear();
        subnodes.clear();
        subnodeMap.clear();

        nodeStateForClientRequests.clear();
        followerSocketAddressBook.clear();
        followerLearnerHandlerMap.clear();
        followerLearnerHandlerSenderMap.clear();

        participants.clear();

        // configure client map
        clientMap.clear();
        keySet.clear();

        // configure network partion info
        partitionMap.clear();

        readRecordIntercepted.clear();

//        partitionMap.addAll(Collections.nCopies(schedulerConfiguration.getNumNodes(),
//                new ArrayList<>(Collections.nCopies(schedulerConfiguration.getNumNodes(), false))));

        for (int i = 0 ; i < nodeNum; i++) {
            nodeStates.add(NodeState.STARTING);
            nodePhases.add(Phase.ELECTION);
            subnodeSets.add(new HashSet<Subnode>());
            lastProcessedZxids.add(0L);
            acceptedEpochs.add(0L);
            currentEpochs.add(0L);
            nodeStateForClientRequests.add(NodeStateForClientRequest.SET_DONE);
            followerSocketAddressBook.add(null);
            followerLearnerHandlerMap.add(null);
            followerLearnerHandlerSenderMap.add(null);

            syncTypeList.add(-1);

            partitionMap.add(new ArrayList<>(Collections.nCopies(nodeNum, false)));

            allZxidRecords.add(new ArrayList<>(Arrays.asList(0L)));

            pendingProposalsInSync.add(new ArrayList<>());
            pendingCommitsInSync.add(new ArrayList<>());
        }

        eventIdGenerator.set(0);
        clientIdGenerator.set(0);

        messageEventMap.clear();
        messageInFlight = 0;
        logRequestInFlight = 0;

        firstMessage.clear();
        firstMessage.addAll(Collections.<Boolean>nCopies(nodeNum, null));

        // Configure lastNodeStartEvents
        lastNodeStartEvents.clear();
        lastNodeStartEvents.addAll(Collections.<NodeStartEvent>nCopies(nodeNum, null));

        // Generate node crash events
        if (schedulerConfiguration.getNumCrashes() > 0) {
            for (int i = 0; i < nodeNum; i++) {
                final NodeCrashEvent nodeCrashEvent = new NodeCrashEvent(generateEventId(), i, nodeCrashExecutor);
                schedulingStrategy.add(nodeCrashEvent);
            }
        }
    }

    /***
     * Configure all testing metadata after election
     */
    private void configureAfterElection(String serverList) {
        // Initialize zkClients
        createClient(1, true, serverList);

        // Reconfigure executors
        nodeStartExecutor = new NodeStartExecutor(this, schedulerConfiguration.getNumRebootsAfterElection());
        nodeCrashExecutor = new NodeCrashExecutor(this, schedulerConfiguration.getNumCrashesAfterElection());
    }

    public int setInitState(final ExternalModelStrategy strategy,
                            final Integer currentStep,
                            final Integer leaderId,
                            final List<Integer> peers,
                            final String serverName,
                            final JSONObject elements,
                            int totalExecuted) throws SchedulerConfigurationException {

//        long modelAcceptedEpoch = getModelAcceptedEpoch(elements, serverName);
        LOG.debug("election leader: {}, other participants: {}",
                leaderId, peers);
        scheduleElectionAndDiscovery(strategy, currentStep, leaderId, peers, null, 1L, totalExecuted);

        long modelZxid = -1L;
        int retry1 = 3;

        for (Integer peer: peers) {
            scheduleLeaderSyncFollower(strategy, leaderId, peer, -1L, totalExecuted);
        }

        // sync
        for (Integer peer: peers) {
            scheduleInternalEventWithWaitingRetry(strategy,
                    ModelAction.FollowerProcessDIFF, peer, leaderId, modelZxid, totalExecuted, retry1);
        }
        for (Integer peer: peers) {
            scheduleFollowerProcessNEWLEADER(strategy, ModelVersion.DEFAULT,
                    peer, leaderId, -1L, totalExecuted);
        }
        for (Integer peer: peers) {
            scheduleFollowerProcessNEWLEADERAfterCurrentEpochUpdated(strategy,
                    ModelVersion.DEFAULT, peer, -1, -1, totalExecuted);
        }
        for (Integer peer: peers) {
            scheduleLeaderProcessACKLD(strategy,
                    ModelAction.LeaderProcessACKLD, leaderId, peer, modelZxid, totalExecuted);
        }
        for (Integer peer: peers) {
            scheduleFollowerProcessUPTODATE(strategy,
                    ModelVersion.DEFAULT, peer, leaderId, 0L, totalExecuted);
       }

        // IN BROADCAST
        // write request
        long lastZxidInLeaderHistory = getLastZxidInNodeHistory(elements, serverName);
        LOG.debug("LeaderProcessRequest modelZxid: {}", Long.toHexString(lastZxidInLeaderHistory));
        totalExecuted = scheduleLeaderProcessRequest(strategy,
                leaderId, leaderId, lastZxidInLeaderHistory, totalExecuted);
        return totalExecuted;

    }


    public long preferLeaderMessageInElection(final Integer leaderId,
                                              Set<Integer> allParticipants,
                                              Long maxElectionEpochInLeader,
                                              Set<Event> otherEvents,
                                              int totalExecuted) throws IOException {
        Set<Event> peerEvents = new HashSet<>(); // delay scheduling non-leader's message
//        Set<Event> otherEvents = new HashSet<>();

        // prefer leader's message
        while (schedulingStrategy.hasNextEvent()) {
            long begintime = System.currentTimeMillis();
            LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
            final Event event = schedulingStrategy.nextEvent();
            // do not schedule other types' events
            if (!(event instanceof ElectionMessageEvent)) {
                LOG.debug(" During election, will not schedule non-election message {}", event);
                otherEvents.add(event);
                continue;
            }
            // do not schedule other nodes' election events
            ElectionMessageEvent e = (ElectionMessageEvent) event;
            final int sendingSubnodeId = e.getSendingSubnodeId();
            final int sendingNodeId = subnodes.get(sendingSubnodeId).getNodeId();
            if (!allParticipants.contains(sendingNodeId)) {
                LOG.debug(" During election, will not schedule non-participants' message {}", event);
                otherEvents.add(event);
                continue;
            }

            // if the receiving node is not a participants, just drop it.
            final int receivingNode = e.getReceivingNodeId();
            if (!allParticipants.contains(receivingNode)) {
                LOG.debug("the receiving node is not a participants, just drop it.");
                e.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
            }

            long electionEpoch = e.getElectionEpoch();
            int proposedLeader = e.getProposedLeader();
            // update leader's electionEpoch
            if (sendingNodeId != leaderId) {
                if (receivingNode == leaderId && electionEpoch > maxElectionEpochInLeader) {
                    LOG.debug("update leader {}'s maxElectionEpoch {} to {}",
                            leaderId, electionEpoch, maxElectionEpochInLeader);
                    maxElectionEpochInLeader = electionEpoch;
                } else {
                    // all nodes should be communicated through leader
                    if (sendingNodeId != receivingNode) {
                        LOG.debug("dropping notifications irrelative to leader {} ,sendingNodeId {}, receivingNode {}.",
                                leaderId, sendingNodeId, receivingNode);
                        e.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                    }
                }
            }

            if (sendingNodeId != leaderId) {
                LOG.debug("peer event: {}", e);
                peerEvents.add(event);
                continue;
            }

//            firstMessage.set(leaderId, null);
            if (event.execute()) {
                recordProperties(totalExecuted, begintime, event);
            }
//            final int receivingNodeId = e.getReceivingNodeId();
//            if (receivingNodeId != schedulerConfiguration.getNumNodes() - 1) {
//                // wait for leader's next election message LOOKING node to come
//                controlMonitor.notifyAll();
//                waitFirstMessageOffered(leaderId);
//            }
        }

        // ensure that there exists no leader event
        for (Event e: peerEvents) {
            LOG.debug("Adding back peer election message that is missed during election: {}", e);
            addEvent(e);
        }

        return maxElectionEpochInLeader;
    }

    /***
     * The schedule process of leader election
     * Pre-condition: all nodes steady
     * Post-condition: all nodes voted & all nodes steady before request
     * Property check: one leader has been elected
     * @param leaderId the leader that model specifies
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    public int scheduleElection(final Integer currentStep, final String line, Integer leaderId, int totalExecuted) {
        try{
//            statistics.startTimer();
            synchronized (controlMonitor) {
                // pre-condition
//                waitAllNodesSteady();
                waitAliveNodesInLookingState();
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    long begintime = System.currentTimeMillis();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    final Event event = schedulingStrategy.nextEvent();
                    if (event instanceof LeaderToFollowerMessageEvent) {
                        final int sendingSubnodeId = ((LeaderToFollowerMessageEvent) event).getSendingSubnodeId();
                        // confirm this works / use partition / let
                        deregisterSubnode(sendingSubnodeId);
                        ((LeaderToFollowerMessageEvent) event).setExecuted();
                        LOG.debug("----Do not let the previous learner handler message occur here! So pass this event---------\n\n\n");
                        continue;
                    }
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, begintime, event);
                    }
                }
                // pre-condition for election property check
                waitAllNodesVoted();
//                waitAllNodesSteadyBeforeRequest();
            }
            statistics.endTimer();
            // check election results
            leaderElectionVerifier.setModelResult(leaderId);
            leaderElectionVerifier.verify();
            // report statistics
            if (currentStep != null && line != null) {
                statistics.reportCurrentStep("[LINE " + currentStep + "]-" + line);
            }
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }


    /***
     * The schedule process of election and discovery
     * Pre-condition: all alive participants steady and in looking state including the leader
     * Post-condition: all nodes voted & all nodes ready for sync
     * Property check: one leader has been elected
     * @param leaderId the leader that model specifies
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    public int scheduleElectionAndDiscovery(ExternalModelStrategy strategy,
                                            final Integer currentStep,
                                            final Integer leaderId,
                                            List<Integer> peers,
                                            List<Integer> looking,
                                            final Long leaderCurrentEpoch,
                                            int totalExecuted) throws SchedulerConfigurationException {
        try{
            Set<Integer> allParticipants = new HashSet<>(peers);
            allParticipants.add(leaderId);
            Set<Integer> lookingParticipants;

            /* Election */
            LOG.debug("start Election! try to elect leader: {}, followers: {}", leaderId, peers);
            synchronized (controlMonitor) {
                // pre-condition: determine which node is looking for leader
                if (looking == null) {
                    lookingParticipants = new HashSet<>(peers);
                    if (participants.size() <= (schedulerConfiguration.getNumNodes() / 2)) {
                        lookingParticipants.add(leaderId);
                    }
                } else {
                    lookingParticipants = new HashSet<>(looking);
                }
                LOG.debug("Nodes looking for leader: {}", lookingParticipants);
                waitAliveNodesInLookingState(lookingParticipants);

                // if model leader still in LOOKING state
                // let leader's election messages to all nodes come first
                if (!leaderElectionStates.get(leaderId).equals(LeaderElectionState.LEADING)) {
                    waitFirstMessageOffered(leaderId);
                }

                // Make all message events during election one single step
                ++totalExecuted;
                boolean electionFinished = false;
                int retry = 0;
                Set<Event> otherEvents = new HashSet<>();
                while (!electionFinished && retry < 20) {
                    retry++;
                    // find max electionEpoch
                    long maxElectionEpochInLeader = 0L;

                    // if leader's message first
//                    LOG.debug("try to schedule model leader {}'s message first if exists.", leaderId);
//                    maxElectionEpochInLeader = preferLeaderMessageInElection(leaderId, allParticipants, maxElectionEpochInLeader, otherEvents, totalExecuted);
//                    otherEvents.addAll(preferLeaderMessageInElection(leaderId, allParticipants, maxElectionEpoch, otherEvents, totalExecuted));
                    LOG.debug("after preferLeaderMessageInElection. maxElectionEpoch: {}, other events size: {} ",
                            maxElectionEpochInLeader, otherEvents.size());
                    while (schedulingStrategy.hasNextEvent()) {
                        long begintime = System.currentTimeMillis();
                        LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
                        final Event event = schedulingStrategy.nextEvent();
                        // do not schedule other types' events
                        if (!(event instanceof ElectionMessageEvent)) {
                            LOG.debug(" During election, will not schedule non-election message {}", event);
                            otherEvents.add(event);
                            continue;
                        }
                        // do not schedule other nodes' election events
                        ElectionMessageEvent e = (ElectionMessageEvent) event;
                        final int sendingSubnodeId = e.getSendingSubnodeId();
                        final int sendingNodeId = subnodes.get(sendingSubnodeId).getNodeId();
                        if (!allParticipants.contains(sendingNodeId)) {
                            LOG.debug(" During election, will not schedule non-participants' message {}", event);
                            otherEvents.add(event);
                            continue;
                        }

                        // if the receiving node is not a participants, just drop it.
                        final int receivingNode = e.getReceivingNodeId();
                        if (!allParticipants.contains(receivingNode)) {
                            LOG.debug("the receiving node is not a participants, just drop it.");
                            e.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                        }

                        long electionEpoch = e.getElectionEpoch();
                        int proposedLeader = e.getProposedLeader();
                        // update leader's electionEpoch
                        if (sendingNodeId != leaderId) {
                            if (receivingNode == leaderId ) {
                                if (electionEpoch > maxElectionEpochInLeader) {
                                    LOG.debug("update leader {}'s maxElectionEpoch {} to {}",
                                            leaderId, electionEpoch, maxElectionEpochInLeader);
                                    maxElectionEpochInLeader = electionEpoch;
                                }
                            } else {
                                // all nodes should be communicated through leader
                                if (sendingNodeId != receivingNode) {
                                    if (lookingParticipants.contains(sendingNodeId) && lookingParticipants.contains(receivingNode) ) {
                                        LOG.debug("dropping notifications irrelative to leader {} ,sendingNodeId {}, receivingNode {}.",
                                                leaderId, sendingNodeId, receivingNode);
                                        e.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                                    } else {
                                        LOG.debug("receivingNode {} NOT in LOOKING state, " +
                                                "should let it communicate vote result", receivingNode);
                                    }
                                }
//                            if (electionEpoch >= maxElectionEpochInLeader && proposedLeader != leaderId && sendingNodeId != receivingNode) {
//                                LOG.debug("electionEpoch {} >= maxElectionEpoch {}, proposedLeader {} != leaderId {}, just drop it.",
//                                        electionEpoch, maxElectionEpochInLeader, proposedLeader, leaderId);
//                                e.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
//                            }

                            }
                        }

                        if (event.execute()) {
                            recordProperties(totalExecuted, begintime, event);
                        }

                    }
                    // pre-condition for election property check
                    electionFinished = waitAllParticipantsVoted(allParticipants);
                }

                // throw SchedulerConfigurationException if leader does not exist
                if (!electionFinished) {
                    LOG.debug("Leader not exist! " +
                            "SchedulerConfigurationException found when scheduling ElectionAndDiscovery." +
                            " leader: " + leaderId + " peers: " + peers);
                    throw new SchedulerConfigurationException();
                }
                if (!LeaderElectionState.LEADING.equals(leaderElectionStates.get(leaderId))) {
                    LOG.debug("Unexpected leader is elected! " +
                            "SchedulerConfigurationException found when scheduling ElectionAndDiscovery." +
                            " leader: " + leaderId + " peers: " + peers);
                    throw new SchedulerConfigurationException();
                }
                participants.addAll(allParticipants);
                for (Event e: otherEvents) {
                    LOG.debug("Adding back event that is missed during election: {}", e);
                    addEvent(e);
                }

                // wait for the event peers' learner handler in sending LEADERINFO
                leaderSyncFollowerCountMap.put(leaderId, peers.size());
                waitLeaderSyncReady(leaderId, peers);
            }

            /* Discovery */
            LOG.debug("Election finished and start DISCOVERY! leader: {}, followers: {}", leaderId, peers);
            // leader releases LEADERINFO
            for (int peer: peers) {
                if (lookingParticipants.contains(peer)) {
                    LOG.debug("About to schedule LeaderSendLEADERINFO to a new follower! leader: {}, follower: {}", leaderId, peer);
                    scheduleInternalEvent(strategy, ModelAction.FollowerProcessLEADERINFO, peer, leaderId,
                            makeZxid(leaderCurrentEpoch, 0), totalExecuted - 1);
                }
            }

            // participant followers releases ACKEPOCH
            // Need to guarantee that at the end leader has updated its currentEpoch here.
            for (Integer peer: peers) {
                if (lookingParticipants.contains(peer)) {
                    LOG.debug("About to schedule FollowerSendACKEPOCH! follower: {}, leader: {}", peer, leaderId);
                    scheduleInternalEventWithWaitingRetry(strategy,
                            ModelAction.LeaderProcessACKEPOCH, leaderId, peer, -1L, totalExecuted - 1, 3);
                }
            }

            statistics.endTimer();
            // check election results
            leaderElectionVerifier.setModelResult(leaderId);
            leaderElectionVerifier.setParticipants(allParticipants);
            leaderElectionVerifier.verify();
            // report statistics
            if (currentStep != null) {
                statistics.reportCurrentStep("[Step " + (currentStep + 1) + "]-"
                        + "ElectionAndDiscovery, leader: " + leaderId
                        + " peers: " + peers);
            }
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");
            if (!traceMatched) {
                LOG.debug("UNMATCH model during ElectionAndDiscovery. " +
                        " leader: " + leaderId + " peers: " + peers);
                throw new SchedulerConfigurationException();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    private int scheduleLeaderSyncFollower(ExternalModelStrategy strategy,
                                           final int leaderId,
                                           final int followerId,
                                           final long modelEpoch,
                                           int totalExecuted) throws SchedulerConfigurationException {
        // step 1. release leader's SyncFollower
        try {
            LOG.debug("try to schedule LeaderSyncFollower. follower: {}", followerId);
            totalExecuted = scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LeaderSyncFollower,
                    leaderId, followerId, modelEpoch, totalExecuted, 1);
        } catch (SchedulerConfigurationException e2) {
            LOG.debug("SchedulerConfigurationException found when scheduling FollowerSendACKEPOCH! " +
                    "Try to schedule FollowerSendACKEPOCH. (This should usually occur in Discovery)");
            throw new SchedulerConfigurationException();
        }
//        // Step 2. release leader's waitForEpochACK
//        try {
//            LOG.debug("try to schedule LeaderWaitForEpochAck leader: {}", leaderId);
//            totalExecuted = scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LeaderWaitForEpochAck,
//                    leaderId, -1, modelEpoch, totalExecuted, 1);
//        } catch (SchedulerConfigurationException e2) {
//            LOG.debug("SchedulerConfigurationException found when scheduling LeaderWaitForEpochAck! " +
//                    "Try to schedule LeaderWaitForEpochAck. (This should usually occur in Discovery)");
//        }
        return totalExecuted;
    }

    private int scheduleFollowerProcessNEWLEADER(ExternalModelStrategy strategy,
                                                 ModelVersion modelVersion,
                                                 final int followerId,
                                                 final int leaderId,
                                                 final long modelZxid,
                                                 int totalExecuted) throws SchedulerConfigurationException {
        LOG.debug("Model version: {}.", modelVersion);
        switch (modelVersion) {
            case DEFAULT:
            case MSPEC_2:
                LOG.debug("Try to schedule FollowerProcessNEWLEADER. " +
                        "followerId: {}, leaderId: {}", followerId, leaderId);
                totalExecuted = scheduleInternalEventWithWaitingRetry(strategy,
                        ModelAction.FollowerProcessNEWLEADER, followerId, leaderId, modelZxid, totalExecuted, 10);
                break;
            case MSPEC_2_5:
            case MSPEC_3:
                LOG.debug("Try to schedule FollowerProcessNEWLEADER and FollowerLogRequestWhenProcessingNEWLEADER. " +
                        "followerId: {}, leaderId: {}", followerId, leaderId);
                scheduleInternalEventWithWaitingRetry(strategy,
                        ModelAction.FollowerProcessNEWLEADER, followerId, leaderId, modelZxid, totalExecuted, 10);
                totalExecuted = scheduleInternalEventWithWaitingRetry(strategy,
                        ModelAction.FollowerLogRequestWhenProcessingNEWLEADER, followerId, -1, modelZxid, totalExecuted, 3);
                break;
            default:
                throw new SchedulerConfigurationException();
        }
        return totalExecuted;

    }

    private int scheduleFollowerProcessNEWLEADERAfterCurrentEpochUpdated(ExternalModelStrategy strategy,
                                                                         ModelVersion modelVersion,
                                                                         final int followerId,
                                                                         final int leaderId,
                                                                         final long modelZxid,
                                                                         int totalExecuted) throws SchedulerConfigurationException {
        LOG.debug("Model version: {}.", modelVersion);
        switch (modelVersion) {
            case DEFAULT:
            case MSPEC_2:
                LOG.debug("Try to schedule FollowerLogRequestWhenProcessingNEWLEADER. " +
                        "followerId: {}, leaderId: {}", followerId, leaderId);
                scheduleInternalEventWithWaitingRetry(strategy,
                        ModelAction.FollowerLogRequestWhenProcessingNEWLEADER,
                        followerId, leaderId, modelZxid, totalExecuted, 5);
                // Log request in SYNC
                List<Long> followerPendingProposals = pendingProposalsInSync.get(followerId);
                Iterator<Long> pendingProposalIterator = followerPendingProposals.listIterator();
                while (pendingProposalIterator.hasNext()) {
                    LOG.debug("Try to schedule FollowerLog in SYNC. " +
                            "followerId: {}, leaderId: {}", followerId, leaderId);
                    scheduleInternalEventWithWaitingRetry(strategy, ModelAction.FollowerLog,
                            followerId, leaderId, pendingProposalIterator.next(), totalExecuted, 3);
                }
                followerPendingProposals.clear();
                totalExecuted++;
                break;
            case MSPEC_3:
                LOG.debug("Do not need to schedule any event!" +
                        "followerId: {}, leaderId: {}", followerId, leaderId);
                totalExecuted = scheduleDummyEvent(totalExecuted);
                break;
            case MSPEC_2_5:
            default:
                throw new SchedulerConfigurationException();
        }
        return totalExecuted;
    }

    private int scheduleLeaderProcessACKLD(ExternalModelStrategy strategy,
                                           ModelAction modelAction,
                                           final int leaderId,
                                           final int followerId,
                                           final long modelZxid,
                                           int totalExecuted) throws SchedulerConfigurationException {
        //  release LearnerHandlerReadRecord
        LOG.debug("readRecordIntercepted: {}", readRecordIntercepted);
        if (readRecordIntercepted.containsKey(followerId) && readRecordIntercepted.get(followerId)) {
            try {
                LOG.debug("try to schedule LearnerHandlerReadRecord from follower: {}", followerId);
                scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LearnerHandlerReadRecord,
                        followerId, leaderId, modelZxid, totalExecuted, 3);
            } catch (SchedulerConfigurationException e2) {
                LOG.debug("SchedulerConfigurationException found when scheduling LearnerHandlerReadRecord! ");
                throw new SchedulerConfigurationException();
            }
        }

        //  release follower's ACK
        try {
            LOG.debug("try to schedule {},  follower: {}, leader: {}", modelAction, followerId, leaderId);
            scheduleInternalEventWithWaitingRetry(strategy, modelAction,
                    leaderId, followerId, modelZxid, totalExecuted, 3);
        } catch (SchedulerConfigurationException e2) {
            LOG.debug("SchedulerConfigurationException found when scheduling {}! ", modelAction);
            throw new SchedulerConfigurationException();
        }
        return totalExecuted + 1;
    }

    private int scheduleFollowerProcessUPTODATE(ExternalModelStrategy strategy,
                                                ModelVersion modelVersion,
                                                final int followerId,
                                                final int leaderId,
                                                final long modelZxid,
                                                int totalExecuted) throws SchedulerConfigurationException {
        LOG.debug("Model version: {}.", modelVersion);
        switch (modelVersion) {
            case DEFAULT:
            case MSPEC_2:
                LOG.debug("Try to schedule FollowerProcessUPTODATE. " +
                        "followerId: {}, leaderId: {}", followerId, leaderId);
                scheduleInternalEventWithWaitingRetry(strategy,
                        ModelAction.FollowerProcessUPTODATE, followerId, leaderId, modelZxid, totalExecuted, 5);

                LOG.debug("Try to schedule LeaderProcessACK. " +
                        "leaderId: {}, followerId: {}", leaderId, followerId);
                scheduleLeaderProcessACK(strategy, leaderId, followerId, modelZxid, totalExecuted);

                // Log request in SYNC
                List<Long> followerPendingProposals = pendingProposalsInSync.get(followerId);
                Iterator<Long> pendingProposalIterator = followerPendingProposals.listIterator();
                while (pendingProposalIterator.hasNext()) {
                    LOG.debug("Try to schedule FollowerLog in SYNC. " +
                            "followerId: {}, leaderId: {}", followerId, leaderId);
                    scheduleInternalEventWithWaitingRetry(strategy, ModelAction.FollowerLog,
                            followerId, leaderId, pendingProposalIterator.next(), totalExecuted, 3);
                }
                followerPendingProposals.clear();
                // Commit in SYNC
                List<Long> followerPendingCommits = pendingCommitsInSync.get(followerId);
                Iterator<Long> pendingCommitIterator = followerPendingCommits.listIterator();
                while (pendingCommitIterator.hasNext()) {
                    LOG.debug("Try to schedule FollowerCommit in SYNC. " +
                            "followerId: {}, leaderId: {}", followerId, leaderId);
                    scheduleInternalEventWithWaitingRetry(strategy, ModelAction.FollowerCommit,
                            followerId, leaderId, pendingCommitIterator.next(), totalExecuted, 3);
                }
                followerPendingCommits.clear();
                totalExecuted++;
                break;
            case MSPEC_2_5:
            case MSPEC_3:
                LOG.debug("Try to schedule FollowerProcessUPTODATE. " +
                        "followerId: {}, leaderId: {}", followerId, leaderId);
                scheduleInternalEventWithWaitingRetry(strategy,
                        ModelAction.FollowerProcessUPTODATE, followerId, leaderId, modelZxid, totalExecuted, 5);

                LOG.debug("Try to schedule LeaderProcessACKofUPTODATE. " +
                        "leaderId: {}, followerId: {}", leaderId, followerId);
                scheduleLeaderProcessACK(strategy, leaderId, followerId, modelZxid, totalExecuted);
                totalExecuted++;
                break;
            default:
                throw new SchedulerConfigurationException();
        }
        return totalExecuted;
    }

    /***
     * create client session
     * Note: when client is initializing, servers are better not allowed to be intercepted
     *      o.w. the initialization may be blocked and fail.
     */
    private void createClient(final int clientId, final boolean resetConnectionState, final String serverList) {
        if (resetConnectionState) {
            clientInitializationDone = false;
        }
        ClientProxy clientProxy = new ClientProxy(this, clientId, serverList);
        clientMap.put(clientId, clientProxy);
        LOG.debug("------------------start the client session initialization------------------");

        clientProxy.start();
        synchronized (controlMonitor) {
            controlMonitor.notifyAll();
            waitClientSessionReady(clientId);
        }
        clientInitializationDone = true;

        // wait for specific time
//        while (!clientProxy.isReady()) {
//            LOG.debug("------------------still initializing the client session------------------");
//            try {
//                Thread.sleep(100);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
        LOG.debug("------------------finish the client session initialization------------------\n");
    }

    /***
     * create client session: for now we only consider connecting to leader
     * Note: when client is initializing, servers are better not allowed to be intercepted
     *      o.w. the initialization may be blocked and fail.
     *
     */
    private void establishSession(ExternalModelStrategy strategy,
                                  final int clientId,
                                  final int leaderId,
                                  int totalExecuted,
                                  final boolean resetConnectionState,
                                  final String serverList) throws SchedulerConfigurationException, IOException {
        synchronized (controlMonitor) {
            // pre-condition
            if (resetConnectionState) {
                clientInitializationDone = false;
            }
            controlMonitor.notifyAll();
//            waitAllNodesSteadyBeforeRequest();
            ClientProxy clientProxy = new ClientProxy(this, clientId, serverList);
            clientMap.put(clientId, clientProxy);

            LOG.debug("------------------start the client session initialization------------------");
            clientProxy.start();

            controlMonitor.notifyAll();
            // post-condition
            waitPrimeConnectionDone(clientId);
        }

        // createSession
        LOG.debug("------------------Processing proposal: createSession------------------");
        scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LeaderLog,
                leaderId, -1, -1, totalExecuted, 5);
        // followerProcessProposal
//        for (int peer: participants) {
//            if (peer == leaderId) continue;
//            scheduleFollowerProcessPROPOSAL(strategy, peer, leaderId, -1, totalExecuted);
//            scheduleLeaderProcessACK(strategy, leaderId, peer, -1, totalExecuted);
//            scheduleFollowerProcessCOMMIT(strategy, peer, leaderId, -1, totalExecuted);
//        }
        for (int peer: participants) {
            if (peer == leaderId) continue;
            scheduleFollowerProcessPROPOSAL(strategy, ModelVersion.DEFAULT, peer, leaderId, -1, totalExecuted);
        }
        for (int peer: participants) {
            if (peer == leaderId) continue;
            scheduleLeaderProcessACK(strategy, leaderId, peer, -1, totalExecuted);
        }
        for (int peer: participants) {
            if (peer == leaderId) continue;
            scheduleFollowerProcessCOMMIT(strategy, ModelVersion.DEFAULT, peer, leaderId, -1, totalExecuted);
        }

        // create key=/test if un-exists
        String key = "/test";
        if (!keySet.contains(key)) {
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.CREATE, key, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                try {
                    if (event.execute()) {
                        recordProperties(totalExecuted + 1, startTime, event);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            LOG.debug("\n\n\n------------------Processing proposal: create key=/test------------------");
            scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LeaderLog,
                    leaderId, -1, -1, totalExecuted, 5);
            // followerProcessProposal
//            for (int peer: participants) {
//                if (peer == leaderId) continue;
//                scheduleFollowerProcessPROPOSAL(strategy, peer, leaderId, -1, totalExecuted);
//                scheduleLeaderProcessACK(strategy, leaderId, peer, -1, totalExecuted);
//                scheduleFollowerProcessCOMMIT(strategy, peer, leaderId, -1, totalExecuted);
//            }
            // followerProcessProposal
            for (int peer: participants) {
                if (peer == leaderId) continue;
                scheduleFollowerProcessPROPOSAL(strategy, ModelVersion.DEFAULT, peer, leaderId, -1, totalExecuted);
            }
            for (int peer: participants) {
                if (peer == leaderId) continue;
                scheduleLeaderProcessACK(strategy, leaderId, peer, -1, totalExecuted);
            }
            for (int peer: participants) {
                if (peer == leaderId) continue;
                scheduleFollowerProcessCOMMIT(strategy, ModelVersion.DEFAULT, peer, leaderId, -1, totalExecuted);
            }

            keySet.add(key);
        }

        synchronized (controlMonitor) {
            LOG.debug("------------------finish the client session initialization------------------\n");
            clientInitializationDone = true;
            controlMonitor.notifyAll();
        }
    }

    private void establishSession(final int clientId,
                                  final boolean resetConnectionState,
                                  final String serverList) {
        synchronized (controlMonitor) {
            // pre-condition
            if (resetConnectionState) {
                clientInitializationDone = false;
            }
//            waitAllNodesSteadyBeforeRequest(); // will not release LeaderJudgingIsRunning
            ClientProxy clientProxy = new ClientProxy(this, clientId, serverList);
            clientMap.put(clientId, clientProxy);

            LOG.debug("------------------start the client session initialization------------------");
            clientProxy.start();
            controlMonitor.notifyAll();
            // post-condition
            waitClientSessionReady(clientId);

            clientInitializationDone = true;

            LOG.debug("------------------finish the client session initialization------------------\n");
        }
    }

    private int scheduleClientSetData(ExternalModelStrategy strategy,
                                      final int clientId,
                                      final int leaderId,
                                      final long modelZxid,
                                      int totalExecuted) throws SchedulerConfigurationException {
        try {
            if (participants.size() <= schedulerConfiguration.getNumNodes() / 2) {
                LOG.debug("no enough participants for client setting data !");
                throw new SchedulerConfigurationException();
            }
            // Step 0. establish session if un-exists
            // for now we only consider connecting to leader
            ClientProxy clientProxy = clientMap.get(clientId);
            if (clientProxy == null || clientProxy.isStop()) {
                String serverAddr = getServerAddr(leaderId);
                LOG.debug("client is going to establish connection with server {}", serverAddr);
                establishSession(strategy, clientId, leaderId, totalExecuted, true, serverAddr);
            }

            // Step 1. setData
            synchronized (controlMonitor) {
//                waitAllNodesSteadyBeforeRequest();
                long startTime = System.currentTimeMillis();
                String data = Long.toHexString(modelZxid);
                Event event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.SET_DATA, data, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * This triggers two events :
     *  --> setData
     *  --> Leader log proposal
     *  --> send LeaderToFollowerMessageEvent(PROPOSAL)
     *  Note: if without specific data, will use eventId as its written string value
     */
    private int scheduleLeaderProcessRequest(ExternalModelStrategy strategy,
                                             final int clientId,
                                             final int leaderId,
                                             final long modelZxid,
                                             int totalExecuted) throws SchedulerConfigurationException {
        // Step 1. setData
        scheduleClientSetData(strategy, clientId, leaderId, modelZxid, totalExecuted);
        // Step 2. Leader log proposal
        totalExecuted = scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LeaderLog,
                leaderId, -1, modelZxid, totalExecuted, 1);

//        // Step 3. release LeaderToFollowerPROPOSAL(PROPOSAL) if partition un-exists
//        totalExecuted = scheduleLeaderToFollowerPROPOSAL(strategy, totalExecuted);

        return totalExecuted;
    }

    private int scheduleLeaderToFollowerPROPOSAL(ExternalModelStrategy strategy,
                                                 int totalExecuted) throws SchedulerConfigurationException {
        try {
            synchronized (controlMonitor) {
                Set<Event> otherEvents = new HashSet<>();
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    long begintime = System.currentTimeMillis();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
                    final Event event = schedulingStrategy.nextEvent();
                    if (! (event instanceof LeaderToFollowerMessageEvent)) {
                        otherEvents.add(event);
                        continue;
                    }
                    LeaderToFollowerMessageEvent e = (LeaderToFollowerMessageEvent) event;
                    if (MessageType.PROPOSAL != e.getType()) {
                        otherEvents.add(event);
                        continue;
                    }
                    if (event.execute()) {
                        recordProperties(totalExecuted, begintime, event);
                    }
                }
                for (Event e: otherEvents) {
                    LOG.debug("Adding back event that is missed during LeaderToFollowerPROPOSAL: {}", e);
                    addEvent(e);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    private int scheduleFollowerProcessPROPOSAL(ExternalModelStrategy strategy,
                                                final ModelVersion modelVersion,
                                                final int followerId,
                                                final int leaderId,
                                                final long modelZxid,
                                                int totalExecuted) throws SchedulerConfigurationException {
        LOG.debug("Model version: {}.", modelVersion);
        switch (modelVersion) {
            case DEFAULT:
            case MSPEC_2:
                // In broadcast, leader release PROPOSAL successfully
                // Or, follower just log request that received in SYNC
                try {
                    LOG.debug("Try to schedule LeaderToFollowerProposal and FollowerLog. " +
                            "followerId: {}, leaderId: {}", followerId, leaderId);
                    scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LeaderToFollowerProposal,
                            followerId, leaderId, modelZxid, totalExecuted, 2);
                } catch (SchedulerConfigurationException e2) {
                    LOG.debug("SchedulerConfigurationException found when scheduling LeaderToFollowerProposal! " +
                            "Model version: {}. " +
                            "Try to schedule follower's LogPROPOSAL. (This should usually occur in / just after SYNC)",
                            modelVersion);
                } finally {
                    // follower log, and wait for follower's SYNC thread sending ACK steady
                    totalExecuted = scheduleInternalEventWithWaitingRetry(strategy, ModelAction.FollowerLog,
                            followerId, -1, modelZxid, totalExecuted, 3);
                }
                break;
            case MSPEC_2_5:
            case MSPEC_3:
                // Do not need to match modelZxid in V4
                try {
                    LOG.debug("Try to schedule LeaderToFollowerProposal. " +
                            "followerId: {}, leaderId: {}", followerId, leaderId);
                    totalExecuted = scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LeaderToFollowerProposal,
                            followerId, leaderId, modelZxid, totalExecuted, 2);
                } catch (SchedulerConfigurationException e2) {
                    LOG.debug("SchedulerConfigurationException found when scheduling LeaderToFollowerProposal! " +
                            "Model version: {}. ", modelVersion);
                }
                break;
            default:
                throw new SchedulerConfigurationException();
        }
        return totalExecuted;
    }

    private int scheduleLeaderProcessACK(ExternalModelStrategy strategy,
                                         final int leaderId,
                                         final int followerId,
                                         final long modelZxid,
                                         int totalExecuted) throws SchedulerConfigurationException {
        long zxid = modelZxid;
        if ((modelZxid & 0xffffffffL) == 0) {
            LOG.debug("Leader {} is processing ack of uptodate", leaderId);
            zxid = 0L;
        }
        try {
            // Step 0. release LearnerHandlerReadRecord
            LOG.debug("readRecordIntercepted: {}", readRecordIntercepted);
            if (readRecordIntercepted.containsKey(followerId) && readRecordIntercepted.get(followerId)) {
                LOG.debug("try to schedule LearnerHandlerReadRecord from follower: {}", followerId);
                scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LearnerHandlerReadRecord,
                        followerId, leaderId, zxid, totalExecuted, 3);
            }

            // Step 1. release follower's ACK
            LOG.debug("try to schedule FollowerToLeaderACK. followerId: {},  leaderId: {}", followerId, leaderId);
            scheduleInternalEventWithWaitingRetry(strategy, ModelAction.FollowerToLeaderACK,
                    leaderId, followerId, zxid, totalExecuted, 3);
        } catch (SchedulerConfigurationException e1) {
            LOG.debug("This will be fine. SchedulerConfigurationException found when scheduling FollowerToLeaderACK! " +
                    "Ensure this zxid {} is processed during sync. ", Long.toHexString(zxid));
        } finally {
            // Step 2. release leader's local commit if not done
            if (zxid != 0L) { // ignore ack of uptodate
                try {
                    LOG.debug("try to schedule LeaderCommit leaderId: {}", leaderId);
                    scheduleInternalEvent(strategy, ModelAction.LeaderCommit, leaderId, -1, zxid, totalExecuted);
                } catch (SchedulerConfigurationException e2) {
                    LOG.debug("This will be fine. SchedulerConfigurationException found when scheduling leader's local commit! " +
                            "Ensure leader already commits this zxid {}", Long.toHexString(zxid));
                }
            }
        }

//        LOG.debug("try to schedule LeaderToFollowerCOMMIT leaderId: {}, followerId: {}", leaderId, followerId);
//        totalExecuted = scheduleInternalEvent(strategy, ModelAction.LeaderToFollowerCOMMIT, leaderId, followerId, totalExecuted);

        return totalExecuted + 1;
    }

    private int scheduleFollowerProcessCOMMIT(ExternalModelStrategy strategy,
                                              final ModelVersion modelVersion,
                                              final int followerId,
                                              final int leaderId,
                                              final long modelZxid,
                                              int totalExecuted) throws SchedulerConfigurationException {
        LOG.debug("Model version: {}.", modelVersion);
        switch (modelVersion) {
            case DEFAULT:
            case MSPEC_2:
                // In broadcast, leader release COMMIT successfully
                // Or, follower just COMMIT in SYNC
                try {
                    LOG.debug("Try to schedule LeaderToFollowerCOMMIT and FollowerCommit. " +
                            "followerId: {}, leaderId: {}", followerId, leaderId);
                    scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LeaderToFollowerCOMMIT,
                            followerId, leaderId, modelZxid, totalExecuted, 1);
                } catch (SchedulerConfigurationException e2) {
                    LOG.debug("SchedulerConfigurationException found when scheduling LeaderToFollowerCOMMIT! " +
                            "Try to schedule follower's FollowerCommit. (This should usually occur in / just after SYNC)");
                } finally {
                    // Step 2. follower log, and wait for follower's SYNC thread sending ACK steady
                    totalExecuted = scheduleInternalEventWithWaitingRetry(strategy, ModelAction.FollowerCommit,
                            followerId, -1, modelZxid, totalExecuted, 1);
                }
                break;
            case MSPEC_2_5:
            case MSPEC_3:
                // Do not need to match modelZxid in V4
                try {
                    LOG.debug("Try to schedule LeaderToFollowerCOMMIT. " +
                            "followerId: {}, leaderId: {}", followerId, leaderId);
                    totalExecuted = scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LeaderToFollowerCOMMIT,
                            followerId, leaderId, -1, totalExecuted, 2);
                } catch (SchedulerConfigurationException e2) {
                    LOG.debug("SchedulerConfigurationException found when scheduling LeaderToFollowerCOMMIT! " +
                            "Model version: {}. ", modelVersion);
                }
                break;
            default:
                throw new SchedulerConfigurationException();
        }
        return totalExecuted;
    }

    /***
     * setData with specific data
     * if without specific data, will use eventId as its written string value
     */
    private int scheduleSetData(final int clientId,
                                final int serverId,
                                final String data,
                                int totalExecuted) {
        try {
            ClientProxy clientProxy = clientMap.get(clientId);
            if (clientProxy == null || clientProxy.isStop()) {
                String serverAddr = getServerAddr(serverId);
                LOG.debug("client establish connection with server {}", serverAddr);
                establishSession(clientId, true, serverAddr);
            }
            synchronized (controlMonitor) {
                waitAllNodesSteadyBeforeRequest();
                long startTime = System.currentTimeMillis();
                Event event;
                if (data == null) {
                    event = new ClientRequestEvent(generateEventId(), clientId,
                            ClientRequestType.SET_DATA, clientRequestExecutor);
                } else {
                    event = new ClientRequestEvent(generateEventId(), clientId,
                            ClientRequestType.SET_DATA, data, clientRequestExecutor);
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * getData for externalModelStrategy
     */
    private int scheduleClientGetData(ExternalModelStrategy strategy,
                                      final Integer currentStep,
                                      final int clientId,
                                      final int serverId,
                                      final long modelResult,
                                      final boolean shutdown,
                                      int totalExecuted) throws SchedulerConfigurationException {
        try {
            ClientProxy clientProxy = clientMap.get(clientId);
            if (clientProxy == null || clientProxy.isStop())  {
                String serverAddr = getServerAddr(serverId);
                LOG.debug("client establish connection with server {}", serverAddr);
                establishSession(strategy, clientId, serverId, totalExecuted, true, serverAddr);
            }
            synchronized (controlMonitor) {
//                waitAllNodesSteadyBeforeRequest();
                long startTime = System.currentTimeMillis();
                Event event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestWaitingResponseExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
            statistics.endTimer();
            // check election results
            LOG.debug("scheduleClientGetData: {}, {}", modelResult, Long.toHexString(modelResult));
            getDataVerifier.setModelResult(Long.toHexString(modelResult));
            getDataVerifier.verify();
            // report statistics
            if (currentStep != null ) {
                statistics.reportCurrentStep("[Step " + (currentStep + 1) + "]-ClientGetData");
            }
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");


            if (shutdown) {
                clientProxy = clientMap.get(clientId);
                LOG.debug("shutting down client {}", clientId);
                clientProxy.shutdown();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * getData
     */
    private int scheduleGetData(final Integer currentStep,
                                final String line,
                                final int clientId,
                                final int serverId,
                                final Integer modelResult,
                                int totalExecuted) {
        try {
            ClientProxy clientProxy = clientMap.get(clientId);
            if (clientProxy == null || clientProxy.isStop())  {
                String serverAddr = getServerAddr(serverId);
                LOG.debug("client establish connection with server {}", serverAddr);
                establishSession(clientId, true, serverAddr);
            }
            synchronized (controlMonitor) {
                waitAllNodesSteadyBeforeRequest();
                long startTime = System.currentTimeMillis();
                Event event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestWaitingResponseExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
            statistics.endTimer();
            // check election results
            getDataVerifier.setModelResult(Long.toHexString(modelResult));
            getDataVerifier.verify();
            // report statistics
            if (currentStep != null && line != null) {
                statistics.reportCurrentStep("[LINE " + currentStep + "]-" + line);
            }
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    public int scheduleDummyEvent(int totalExecuted) {
        try {
            long startTime = System.currentTimeMillis();
            Event event = new DummyEvent();
            LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
            LOG.debug("prepare to execute event: {}", event);
            if (event.execute()) {
                ++totalExecuted;
                recordProperties(totalExecuted, startTime, event);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * The schedule process after the first round of election
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    private int scheduleAfterElection(int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                waitAllNodesSteady();
                LOG.debug("All Nodes steady");
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    long begintime = System.currentTimeMillis();
                    for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                        LOG.debug("--------------->Node Id: {}, NodeState: {}, " +
                                        "role: {}, " +
                                        "vote: {}",nodeId, nodeStates.get(nodeId),
                                leaderElectionStates.get(nodeId),
                                votes.get(nodeId)
                        );
                    }
                    executionWriter.write("\n\n---Step: " + totalExecuted + "--->");
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
                    final Event event = schedulingStrategy.nextEvent();
                    // Only the leader will be crashed
                    if (event instanceof NodeCrashEvent){
                        LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(((NodeCrashEvent) event).getNodeId());
                        LOG.debug("----role: {}---------", RoleOfCrashNode);
                        if ( RoleOfCrashNode != LeaderElectionState.LEADING){
                            ((NodeCrashEvent) event).setExecuted();
                            LOG.debug("----pass this event---------\n\n\n");
                            continue;
                        }
                        if (event.execute()) {
                            ++totalExecuted;

                            // wait for new message from online nodes after the leader crashed
                            waitNewMessageOffered();

                            long endtime=System.currentTimeMillis();
                            long costTime = (endtime - begintime);
                            executionWriter.write("-------waitNewMessageOffered cost_time: " + costTime + "\n");
                        }
                    }
                    else if (event.execute()) {
                        ++totalExecuted;
                        long endtime=System.currentTimeMillis();
                        long costTime = (endtime - begintime);
                        executionWriter.write("------cost_time: " + costTime + "\n");
                    }
                }
                waitAllNodesVoted();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * The schedule process after the first round of election with client requests
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    private int scheduleClientRequests(int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                waitAllNodesSteadyBeforeRequest();
                LOG.debug("All Nodes steady for client requests");
                for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
                    executionWriter.write(lastProcessedZxids.get(nodeId).toString() + " # ");
                }
                for (int i = 1; i <= schedulerConfiguration.getNumClientRequests() ; i++) {
                    long begintime = System.currentTimeMillis();
                    executionWriter.write("\n\n---Step: " + totalExecuted + "--->\n");
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
                    LOG.debug("Client request: {}", i);
                    final Event event = schedulingStrategy.nextEvent();
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event.execute()) {
                        ++totalExecuted;
                        LOG.debug("executed event: {}", event.toString());
                        long endtime=System.currentTimeMillis();
                        long costTime = (endtime - begintime);
                        executionWriter.write("-----cost_time: " + costTime + "\n");
                        // Property verification
                        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
                            executionWriter.write(lastProcessedZxids.get(nodeId).toString() + " # ");
                        }
                    }
                }
//                executionWriter.write("---Step: " + totalExecuted + "--->");
//                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
//                LOG.debug("Client request: set data");
//                final Event event = schedulingStrategy.nextEvent();
//                LOG.debug("prepare to execute event: {}", event.toString());
//                if (event.execute()) {
//                    ++totalExecuted;
//                    LOG.debug("executed event: {}", event.toString());
//                }

//                nodeStartExecutor = new NodeStartExecutor(this, executionWriter, 1);
//                nodeCrashExecutor = new NodeCrashExecutor(this, executionWriter, 1);
//                final NodeCrashEvent nodeCrashEvent0 = new NodeCrashEvent(generateEventId(), 0, nodeCrashExecutor);
//                schedulingStrategy.add(nodeCrashEvent0);
//                final NodeCrashEvent nodeCrashEvent1 = new NodeCrashEvent(generateEventId(), 1, nodeCrashExecutor);
//                schedulingStrategy.add(nodeCrashEvent1);
//
//                executionWriter.write("---Step: " + totalExecuted + "--->");
//                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
//                LOG.debug("prepare to execute event: {}", nodeCrashEvent0.toString());
//                if (nodeCrashEvent0.execute()) {
//                    ++totalExecuted;
//                    LOG.debug("executed event: {}", nodeCrashEvent0.toString());
//                }
//
//                executionWriter.write("---Step: " + totalExecuted + "--->");
//                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
//                LOG.debug("prepare to execute event: {}", nodeCrashEvent1.toString());
//                if (nodeCrashEvent1.execute()) {
//                    ++totalExecuted;
//                    LOG.debug("executed event: {}", nodeCrashEvent1.toString());
//                }

                waitAllNodesVoted();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    public int scheduleInternalEventWithRetry(ExternalModelStrategy strategy,
                                              final ModelAction action,
                                              final int nodeId,
                                              final int peerId,
                                              final long modelZxid,
                                              int totalExecuted,
                                              int retry) throws SchedulerConfigurationException {
        while (true) {
            try {
                synchronized (controlMonitor) {
                    long startTime = System.currentTimeMillis();
                    Event event = strategy.getNextInternalEvent(action, nodeId, peerId, modelZxid);
                    assert event != null;
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                break;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            } catch (SchedulerConfigurationException e2) {
                LOG.debug("SchedulerConfigurationException found when scheduling {}! Retry: {}", action, retry);
                retry--;
                if (retry <= 0) {
                    throw e2;
                }
            }
        }
        return totalExecuted;
    }

    /**
     *
     * @param strategy
     * @param action
     * @param nodeId receiver / processing node id
     * @param peerId sender node id
     * @param modelZxid
     * @param totalExecuted
     * @param retry
     * @return
     * @throws SchedulerConfigurationException
     */
    public int scheduleInternalEventWithWaitingRetry(ExternalModelStrategy strategy,
                                                     final ModelAction action,
                                                     final int nodeId,
                                                     final int peerId,
                                                     final long modelZxid,
                                                     int totalExecuted, int retry) throws SchedulerConfigurationException {
        while (retry > 0) {
            try {
                synchronized (controlMonitor) {
                    long startTime = System.currentTimeMillis();
                    controlMonitor.notifyAll();
                    Event event = waitTargetInternalEventReady(strategy, action, nodeId, peerId, modelZxid);
                    if (event == null) {
                        retry--;
                        LOG.debug("target internal event not found! will wait with retry {} more time(s).", retry);
                        continue;
                    }
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                break;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
        if (retry <= 0) {
            LOG.debug("SchedulerConfigurationException found when scheduling {}! Retry: {}", action, retry);
            throw new SchedulerConfigurationException();
        }
        return totalExecuted;
    }

    public int scheduleInternalEvent(ExternalModelStrategy strategy,
                                     final ModelAction action,
                                     final int nodeId,
                                     final int peerId,
                                     final long modelZxid,
                                     int totalExecuted) throws SchedulerConfigurationException {
        try {
            synchronized (controlMonitor) {
//                    waitTargetInternalEventReady(strategy, action, nodeId, peerId);
                long startTime = System.currentTimeMillis();
                Event event = strategy.getNextInternalEvent(action, nodeId, peerId, modelZxid);
                assert event != null;
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SchedulerConfigurationException e2) {
            LOG.debug("SchedulerConfigurationException found when scheduling {}!", action);
            throw e2;
        }
        return totalExecuted;
    }

    public int scheduleInternalEventInSequence(EventSequenceStrategy strategy,
                                               String[] lineArr,
                                               int totalExecuted) throws SchedulerConfigurationException {
        try {
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = strategy.getNextInternalEvent(lineArr);
                assert event != null;
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SchedulerConfigurationException e2) {
            LOG.debug("SchedulerConfigurationException found when scheduling {}!", Arrays.toString(lineArr));
            throw e2;
        }
        return totalExecuted;
    }


    /***
     * log request
     */
    private int scheduleLogRequest(final int nodeId, int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = schedulingStrategy.nextEvent();
                // may be trapped in loop
                while (!(event instanceof LocalEvent) ||
                        !(SubnodeType.SYNC_PROCESSOR.equals(((LocalEvent) event).getSubnodeType())) ||
                        ((LocalEvent) event).getNodeId() != nodeId){
                    LOG.debug("-------need node {} 's SyncRequestEvent! get event: {}", nodeId, event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * commit
     */
    private int scheduleCommit(final int nodeId, int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = schedulingStrategy.nextEvent();
                // this may be trapped in loop
                while (!(event instanceof LocalEvent) ||
                        !(SubnodeType.COMMIT_PROCESSOR.equals(((LocalEvent) event).getSubnodeType())) ||
                        ((LocalEvent) event).getNodeId() != nodeId){
                    LOG.debug("-------need node {} 's CommitEvent! get event: {}", nodeId, event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * schedule learner handler event
     */
    private int scheduleLearnerHandlerMessage(final int sendingNode, final int receivingNode, int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = schedulingStrategy.nextEvent();
                // this may be trapped in loop
                while (!(event instanceof LeaderToFollowerMessageEvent) ||
                        ((LeaderToFollowerMessageEvent) event).getReceivingNodeId() != receivingNode){
                    LOG.debug("-------need node {} 's LeaderToFollowerMessageEvent to node {}! get event: {}",
                            sendingNode, receivingNode, event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * Node crash
     */
    private int scheduleNodeCrash(int nodeId, int totalExecuted) {
        try {
            assert NodeState.ONLINE.equals(nodeStates.get(nodeId));
            // TODO: move this to configuration file
            if (!nodeCrashExecutor.hasCrashes()) {
                nodeCrashExecutor = new NodeCrashExecutor(this, 1);
            }
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = new NodeCrashEvent(generateEventId(), nodeId, nodeCrashExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * Node start
     */
    private int scheduleNodeStart(int nodeId, int totalExecuted) {
        try {
            assert NodeState.OFFLINE.equals(nodeStates.get(nodeId));
            if (!nodeStartExecutor.hasReboots()) {
                nodeStartExecutor = new NodeStartExecutor(this, 1);
            }
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = new NodeStartEvent(generateEventId(), nodeId, nodeStartExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }


    private int schedulePartitionStart(int node1, int node2, int totalExecuted) {
        try {
            assert !partitionMap.get(node1).get(node2);
            assert !partitionMap.get(node2).get(node1);
            if (!partitionStartExecutor.enablePartition()) {
                partitionStartExecutor = new PartitionStartExecutor(this, 1);
            }
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = new PartitionStartEvent(generateEventId(), node1, node2, partitionStartExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    private int schedulePartitionStop(int node1, int node2, int totalExecuted) {
        try {
            assert partitionMap.get(node1).get(node2);
            assert partitionMap.get(node2).get(node1);
            if (!partitionStopExecutor.enablePartitionStop()) {
                partitionStopExecutor = new PartitionStopExecutor(this, 1);
            }
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = new PartitionStopEvent(generateEventId(), node1, node2, partitionStopExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * steps to trigger DIFF
     * @param totalExecuted the first step sequence number
     * @return the last step sequence number
     */
    private int triggerDiffByLeaderRestart(int totalExecuted, final int clientId) {
        try {
            long startTime;
            Event event;
            synchronized (controlMonitor) {
                // PRE
                // >> client get request
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // >> client set request
                startTime = System.currentTimeMillis();
                ClientRequestEvent mutationEvent = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.SET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", mutationEvent);
                if (mutationEvent.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, mutationEvent);
                }

                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }

//                // >> client get request
//                startTime = System.currentTimeMillis();
//                event = new ClientRequestEvent(generateEventId(), clientId,
//                        ClientRequestType.GET_DATA, clientRequestExecutor);
//                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
//                LOG.debug("prepare to execute event: {}", event);
//                if (event.execute()) {
//                    ++totalExecuted;
//                    recordProperties(totalExecuted, startTime, event);
//                }

                // TODO: check quorum nodes has updated lastProcessedZxid whenever a client mutation is done
//                waitQuorumZxidUpdated();

                // in case the previous COMMIT REQUEST EVENT blocked the later execution
                waitResponseForClientRequest(mutationEvent);

                // make DIFF: client set >> leader log >> leader restart >> re-election
                // Step 1. client request SET_DATA
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.SET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 2. leader log
                startTime = System.currentTimeMillis();
                event = schedulingStrategy.nextEvent();
                while (!(event instanceof LocalEvent)){
                    LOG.debug("-------need SyncRequestEvent! get event: {}", event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 3. Leader crash
                startTime = System.currentTimeMillis();
                // find leader
                int leader = -1;
                for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                    if (LeaderElectionState.LEADING.equals(leaderElectionStates.get(nodeId))) {
                        leader = nodeId;
                        LOG.debug("-----current leader: {}", leader);
                        break;
                    }
                }
                event = new NodeCrashEvent(generateEventId(), leader, nodeCrashExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 4. Leader restart
                startTime = System.currentTimeMillis();
                event = schedulingStrategy.nextEvent();
                while (true) {
                    if (event instanceof NodeStartEvent) {
                        final int nodeId = ((NodeStartEvent) event).getNodeId();
                        LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(nodeId);
                        LOG.debug("----previous role of this crashed node {}: {}---------", nodeId, RoleOfCrashNode);
                        if (nodeId == leader){
                            break;
                        }
                    }
                    LOG.debug("-------need NodeStartEvent of the previous crashed leader! add this event back: {}", event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 5. re-election and becomes leader
                // >> This part is also for random test
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                // wait whenever an election ends
                waitAllNodesVoted();
//                leaderElectionVerifier.verify();
                waitAllNodesSteadyBeforeRequest();

            }
            statistics.endTimer();
            // check election results
            leaderElectionVerifier.verify();
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    private int triggerDiffByFollowersRestart(int totalExecuted, final int clientId) {
        try {
            long startTime;
            Event event;
            synchronized (controlMonitor) {
                // make DIFF: client set >> leader log >> leader restart >> re-election
                // Step 1. client request SET_DATA
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.SET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 2. leader log
                startTime = System.currentTimeMillis();
                event = schedulingStrategy.nextEvent();
                while (!(event instanceof LocalEvent)){
                    LOG.debug("-------need SyncRequestEvent! get event: {}", event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // follower crash and restart first, then leader crash
                // distinguish leader and followers
                int leader = -1;
                List<Integer> followersId = new ArrayList<>();
                for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                    switch (leaderElectionStates.get(nodeId)) {
                        case LEADING:
                            leader = nodeId;
                            LOG.debug("-----current leader: {}", nodeId);
                            break;
                        case FOLLOWING:
                            LOG.debug("-----find a follower: {}", nodeId);
                            followersId.add(nodeId);
                    }
                }

                // Step 3. each follower crash and restart
                for (int followerId: followersId) {

                    // follower crash
                    startTime = System.currentTimeMillis();
                    event = new NodeCrashEvent(generateEventId(), followerId, nodeCrashExecutor);
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }

                    // follower restart
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    while (true) {
                        if (event instanceof NodeStartEvent) {
                            final int nodeId = ((NodeStartEvent) event).getNodeId();
                            LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(nodeId);
                            LOG.debug("----previous role of this crashed node {}: {}---------", nodeId, RoleOfCrashNode);
                            if (nodeId == followerId) {
                                break;
                            }
                        }
                        LOG.debug("-------need NodeStartEvent of the previous crashed follower! add this event back: {}", event);
                        addEvent(event);
                        event = schedulingStrategy.nextEvent();
                    }
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }

//                    //restart follower: LOOKING -> FOLLOWING
//                    while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
//                        startTime = System.currentTimeMillis();
//                        event = schedulingStrategy.nextEvent();
//                        LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
//                        LOG.debug("prepare to execute event: {}", event.toString());
//                        if (event instanceof LeaderToFollowerMessageEvent) {
//                            final int sendingSubnodeId = ((LeaderToFollowerMessageEvent) event).getSendingSubnodeId();
//                            // confirm this works / use partition / let
//                            deregisterSubnode(sendingSubnodeId);
//                            ((LeaderToFollowerMessageEvent) event).setExecuted();
//
////                        schedulingStrategy.remove(event);
//                            LOG.debug("----Do not let the previous learner handler message occur here! So pass this event---------\n\n\n");
//                            continue;
//                        }
//                        else if (!(event instanceof ElectionMessageEvent)) {
//                            LOG.debug("not message event: {}, " +
//                                    "which means this follower begins to sync", event.toString());
//                            addEvent(event);
//                            if (leaderElectionStates.get(followerId).equals(LeaderElectionState.FOLLOWING)){
//                                break;
//                            }
//                        }
//                        else if (event.execute()) {
//                            ++totalExecuted;
//                            recordProperties(totalExecuted, startTime, event);
//                        }
//                    }
                }

                // wait Leader becomes LOOKING state
                waitAliveNodesInLookingState();

                // Step 4. re-election and becomes leader again
                // >> This part allows random schedulers
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event instanceof LeaderToFollowerMessageEvent) {
                        final int sendingSubnodeId = ((LeaderToFollowerMessageEvent) event).getSendingSubnodeId();
                        // confirm this works / use partition / let
                        deregisterSubnode(sendingSubnodeId);
                        ((LeaderToFollowerMessageEvent) event).setExecuted();
                        LOG.debug("----Do not let the previous learner handler message occur here! So pass this event---------\n\n\n");
                        continue;
                    }
//                    else if (event instanceof ElectionMessageEvent) {
//                        ElectionMessageEvent event1 = (ElectionMessageEvent) event;
//                        final int sendingSubnodeId = event1.getSendingSubnodeId();
//                        final int receivingNodeId = event1.getReceivingNodeId();
//                        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
//                        final int sendingNodeId = sendingSubnode.getNodeId();
//                        if (sendingNodeId != leader) {
//                            LOG.debug("need leader message event: {}", event.toString());
//                            addEvent(event);
//                            continue;
//                        }
//                    }
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                // wait whenever an election ends
                waitAllNodesVoted();
//                leaderElectionVerifier.verify();
                waitAllNodesSteadyBeforeRequest();

            }
            statistics.endTimer();
            // check election results
            leaderElectionVerifier.verify();
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }


    /***
     * pre-steps to trigger phantom read
     * @param totalExecuted the first step sequence number
     * @return the last step sequence number
     */
    private int followersRestartAndLeaderCrash(int totalExecuted) {
        try {
            Event event;
            long startTime;
            synchronized (controlMonitor){
                // follower crash and restart first, then leader crash
                // distinguish leader and followers
                int leader = -1;
                List<Integer> followersId = new ArrayList<>();
                for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                    switch (leaderElectionStates.get(nodeId)) {
                        case LEADING:
                            leader = nodeId;
                            LOG.debug("-----current leader: {}", nodeId);
                            break;
                        case FOLLOWING:
                            LOG.debug("-----find a follower: {}", nodeId);
                            followersId.add(nodeId);
                    }
                }

                // each follower crash and restart
                for (int followerId: followersId) {
                    startTime = System.currentTimeMillis();
                    event = new NodeCrashEvent(generateEventId(), followerId, nodeCrashExecutor);
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }

                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    while (true) {
                        if (event instanceof NodeStartEvent) {
                            final int nodeId = ((NodeStartEvent) event).getNodeId();
                            LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(nodeId);
                            LOG.debug("----previous role of this crashed node {}: {}---------", nodeId, RoleOfCrashNode);
                            if (nodeId == followerId){
                                break;
                            }
                        }
                        LOG.debug("-------need NodeStartEvent of the previous crashed follower! add this event back: {}", event);
                        addEvent(event);
                        event = schedulingStrategy.nextEvent();
                    }
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }

                // leader crash
                startTime = System.currentTimeMillis();
                event = new NodeCrashEvent(generateEventId(), leader, nodeCrashExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Do not let the crashed leader restart
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event instanceof NodeStartEvent) {
                        final int nodeId = ((NodeStartEvent) event).getNodeId();
                        LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(nodeId);
                        LOG.debug("----previous role of this crashed node {}: {}---------", nodeId, RoleOfCrashNode);
                        if ( LeaderElectionState.LEADING.equals(RoleOfCrashNode)){
                            ((NodeStartEvent) event).setExecuted();
                            LOG.debug("----Do not let the previous leader restart here! So pass this event---------\n\n\n");
                            continue;
                        }
                    }
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                // wait whenever an election ends
                waitAllNodesVoted();
                waitAllNodesSteadyBeforeRequest();
            }
            statistics.endTimer();
            // check election results
            leaderElectionVerifier.verify();
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * only issue GET_DATA without waiting for response
     */
    private int getDataTestNoWait(int totalExecuted, final int clientId) {
        try {
            long startTime;
            Event event;
            synchronized (controlMonitor) {
                // PRE
                // >> client get request
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    private int getDataTest(int totalExecuted, final int clientId) {
        try {
            long startTime;
            Event event;
            synchronized (controlMonitor) {
                // PRE
                // >> client get request
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestWaitingResponseExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    public void addEvent(final Event event) {
        schedulingStrategy.add(event);
    }

    @Override
    public int offerElectionMessage(final int sendingSubnodeId, final int receivingNodeId,
                                    final long electionEpoch, final int leader,
                                    final Set<Integer> predecessorMessageIds, final String payload) {
        final List<Event> predecessorEvents = new ArrayList<>();
//        for (final int messageId : predecessorMessageIds) {
//            predecessorEvents.add(messageEventMap.get(messageId));
//        }
        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
        final int sendingNodeId = sendingSubnode.getNodeId();

        if (nodeStates.get(sendingNodeId).equals(NodeState.OFFLINE) ||
                nodeStates.get(sendingNodeId).equals(NodeState.STOPPING)) {
            return TestingDef.RetCode.NODE_CRASH;
        }

        // We want to determinize the order in which the first messages are added, so we wait until
        // all nodes with smaller ids have offered their first message.
        synchronized (controlMonitor) {
            if (sendingNodeId > 0 && firstMessage.get(sendingNodeId - 1) == null) {
                controlMonitor.notifyAll();
                waitFirstMessageOffered(sendingNodeId - 1);
            }
        }

//        // Problem: It will make the message that come immediately after the node restarts to be missed
//        if (partitionMap.get(sendingNodeId).get(receivingNodeId)){
//            return TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
//        }

//        final NodeStartEvent lastNodeStartEvent = lastNodeStartEvents.get(sendingNodeId);
//        if (null != lastNodeStartEvent) {
//            predecessorEvents.add(lastNodeStartEvent);
//        }

        int id = generateEventId();
        final ElectionMessageEvent electionMessageEvent =
                new ElectionMessageEvent(id, sendingSubnodeId, receivingNodeId, electionEpoch, leader, payload, electionMessageExecutor);
        electionMessageEvent.addAllDirectPredecessors(predecessorEvents);

        synchronized (controlMonitor) {
            LOG.debug("Node {} is offering a message: msgId = {}, predecessors = {}, " +
                            "set subnode {} to SENDING state", sendingNodeId,
                    id, predecessorMessageIds.toString(), sendingSubnodeId);
            messageEventMap.put(id, electionMessageEvent);
            addEvent(electionMessageEvent);
            if (firstMessage.get(sendingNodeId) == null) {
                LOG.debug("set {}'s firstMessage true", sendingNodeId);
                firstMessage.set(sendingNodeId, true);
            }
            sendingSubnode.setState(SubnodeState.SENDING);
            controlMonitor.notifyAll();

            waitMessageReleased(id, sendingNodeId, sendingSubnodeId, electionMessageEvent);
            if (electionMessageEvent.getFlag() == TestingDef.RetCode.EXIT) {
                LOG.debug(" Test trace is over. Drop the event {}", electionMessageEvent);
                if (sendingSubnode.getState().equals(SubnodeState.PROCESSING)) {
                    sendingSubnode.setState(SubnodeState.RECEIVING);
                }
                controlMonitor.notifyAll();
                return TestingDef.RetCode.EXIT;
            }

            try {
                // normally, this event is released when scheduled except:
                // case 1: this event is released since the sending node is crashed
                if (NodeState.STOPPING.equals(nodeStates.get(sendingNodeId)) ) {
                    LOG.debug("----------node {} is crashed! setting ElectionMessage executed and removed. {}",
                            sendingNodeId, electionMessageEvent);
                    executionWriter.write("event id=" + id + " removed due to sending node STOPPING");
                    id = TestingDef.RetCode.NODE_CRASH;
                    electionMessageEvent.setExecuted();
                    schedulingStrategy.remove(electionMessageEvent);

                }
                // case 2: this event is released since the sending subnode is unregistered
                else if (SubnodeState.UNREGISTERED.equals(subnodes.get(sendingSubnodeId).getState())) {
                    LOG.debug("----------subnode {} of node {} is unregistered! setting ElectionMessage executed and removed. {}",
                            sendingSubnodeId, sendingNodeId, electionMessageEvent);
                    executionWriter.write("event id=" + id + " removed due to unregistered");
                    id = TestingDef.RetCode.SUBNODE_UNREGISTERED;
                    electionMessageEvent.setExecuted();
                    schedulingStrategy.remove(electionMessageEvent);
                }
                // case 3: this event is released when the network partition occurs
                else if (partitionMap.get(sendingNodeId).get(receivingNodeId) ||
                        electionMessageEvent.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION) {
                    LOG.debug("----------node {} and {} get partitioned! setting ElectionMessage executed and removed. {}",
                            sendingNodeId, receivingNodeId, electionMessageEvent);
                    executionWriter.write("\nevent id=" + id + " removed due to partition");
                    id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                    electionMessageEvent.setExecuted();
                    schedulingStrategy.remove(electionMessageEvent);
                }
                else if (NodeState.UNREADY.equals(nodeStates.get(sendingNodeId)) ) {
                    LOG.debug("----------node {} is about to be looking! setting ElectionMessage executed and removed. {}",
                            sendingNodeId, electionMessageEvent);
                    executionWriter.write("event id=" + id + " removed due to become looking");
                    id = TestingDef.RetCode.BACK_TO_LOOKING;
                    electionMessageEvent.setExecuted();
                    schedulingStrategy.remove(electionMessageEvent);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            controlMonitor.notifyAll();

        }

        return id;
    }

    @Override
    public int offerFollowerToLeaderMessage(int sendingSubnodeId, long zxid, String payload, int lastReceivedType) throws RemoteException {
        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
        final int sendingNodeId = sendingSubnode.getNodeId();

        LOG.debug("Follower {} offerFollowerToLeaderMessage. last received type: {}", sendingNodeId, lastReceivedType);

        // record critical info
        int receivingNodeId = leaderElectionStates.indexOf(LeaderElectionState.LEADING);
        synchronized (controlMonitor) {
            if (lastReceivedType == MessageType.UPTODATE) {
                LOG.debug("----set follower {} to BROADCAST phase !---", sendingNodeId);
                nodePhases.set(sendingNodeId, Phase.BROADCAST);
            }

            controlMonitor.notifyAll();
            //        // not in partition
//        if (!clientInitializationDone) {
//            LOG.debug("----client initialization is not done!---");
//            return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
//        }

            // check who is the leader, or the leader has crashed
//        assert leaderElectionStates.contains(LeaderElectionState.LEADING);

            if (receivingNodeId == -1) {
                LOG.debug("Leader has crashed / un-exist!");
                return TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
            }


            if (nodeStates.get(sendingNodeId).equals(NodeState.OFFLINE)
                    || nodeStates.get(sendingNodeId).equals(NodeState.STOPPING)) {
                return TestingDef.RetCode.NODE_CRASH;
            }

            if ( nodeStates.get(sendingNodeId).equals(NodeState.UNREADY)){
                LOG.debug("Will not intercept follower {} replying leader {}'s type={} message as " +
                        "it is about to be looking...", sendingNodeId, receivingNodeId, lastReceivedType);
                return TestingDef.RetCode.BACK_TO_LOOKING;
            }

//        // if partition occurs, just return without sending this message
//        // mute the effect of events before adding it to the scheduling list
            if (partitionMap.get(sendingNodeId).get(receivingNodeId)){
                LOG.debug("Follower {} is trying to reply leader {}'s type={} message " +
                        "where they are partitioned. Just return", sendingNodeId, receivingNodeId, lastReceivedType);
                return TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
            }

            switch (lastReceivedType) {
                case MessageType.LEADERINFO:
                    LOG.debug("-------follower reply ACK to LEADERINFO in DISCOVERY phase!");
                    break;
                case MessageType.NEWLEADER:
                    LOG.debug("-------follower reply ACK to NEWLEADER in SYNC phase!");
                    break;
                case MessageType.UPTODATE:
                    LOG.debug("-------follower {} is about to reply ACK to UPTODATE in sync phase!", sendingNodeId);
//                    return TestingDef.RetCode.NOT_INTERCEPTED;
                    break;
                case MessageType.PROPOSAL:
                    LOG.debug("-------follower is about to reply ACK to PROPOSAL in BROADCAST phase!");
                    break;
                case MessageType.PROPOSAL_IN_SYNC:
                    LOG.debug("-------follower is about to reply ACK to PROPOSAL that should be processed in SYNC phase!");
                    break;
                default:
                    LOG.debug("-------follower is sending a message that will not be intercepted!");
                    return TestingDef.RetCode.NOT_INTERCEPTED;
            }
        }

        // intercept the event
        int id = generateEventId();
        final List<Event> predecessorEvents = new ArrayList<>();
        final FollowerToLeaderMessageEvent messageEvent = new FollowerToLeaderMessageEvent(
                id, sendingSubnodeId, receivingNodeId, lastReceivedType, zxid, payload, followerToLeaderMessageExecutor);
        messageEvent.addAllDirectPredecessors(predecessorEvents);

        synchronized (controlMonitor) {
            LOG.debug("Follower {} is replying to last type={} message from leader {}: msgId = {}, " +
                    "set subnode {} to SENDING state", sendingNodeId, lastReceivedType, receivingNodeId, id, sendingSubnodeId);

            addEvent(messageEvent);
            sendingSubnode.setState(SubnodeState.SENDING);
            controlMonitor.notifyAll();

            waitMessageReleased(id, sendingNodeId, sendingSubnodeId, messageEvent);
            if (messageEvent.getFlag() == TestingDef.RetCode.EXIT) {
                LOG.debug(" Test trace is over. Drop the event {}", messageEvent);
                if (sendingSubnode.getState().equals(SubnodeState.PROCESSING)) {
                    sendingSubnode.setState(SubnodeState.RECEIVING);
                }
                controlMonitor.notifyAll();
                return TestingDef.RetCode.EXIT;
            }

            // normally, this event is released when scheduled except:
            // case 1: this event is released since the sending node is crashed
            if (NodeState.STOPPING.equals(nodeStates.get(sendingNodeId))) {
                LOG.debug("----------node {} is crashed! setting followerMessage executed and removed. {}",
                        sendingNodeId, messageEvent);
                id = TestingDef.RetCode.NODE_CRASH;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            // case 2: this event is released since the sending subnode is unregistered
            else if (SubnodeState.UNREGISTERED.equals(subnodes.get(sendingSubnodeId).getState())) {
                LOG.debug("----------subnode {} of node {} is unregistered! setting followerMessage executed and removed. {}",
                        sendingSubnode, sendingNodeId, messageEvent);
                id = TestingDef.RetCode.SUBNODE_UNREGISTERED;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            // case 3: this event is released when the network partition occurs
            else if (partitionMap.get(sendingNodeId).get(receivingNodeId) ||
                    messageEvent.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION) {
                LOG.debug("----------node {} and {} get partitioned! setting messageEvent executed and removed. {}",
                        sendingNodeId, receivingNodeId, messageEvent);
                sendingSubnode.setState(SubnodeState.RECEIVING);
                id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            else if (NodeState.UNREADY.equals(nodeStates.get(sendingNodeId))) {
                LOG.debug("----------node {} is about to become looking! setting followerMessage executed and removed. {}",
                        sendingNodeId, messageEvent);
                id = TestingDef.RetCode.BACK_TO_LOOKING;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            controlMonitor.notifyAll();
        }
        return id;
    }

    @Override
    public int offerLeaderToFollowerMessage(int sendingSubnodeId, String receivingAddr, long zxid, String payload, int type) throws RemoteException {

        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
        final int sendingNodeId = sendingSubnode.getNodeId();

        // PRE-Process
        // record critical info: follower address & follower-learnerHandler relationship
        int receivingNodeId;
        synchronized (controlMonitor) {
            if (!followerSocketAddressBook.contains(receivingAddr)) {
                controlMonitor.notifyAll();
                waitFollowerSocketAddrRegistered(receivingAddr);
            }
            receivingNodeId = followerSocketAddressBook.indexOf(receivingAddr);
            LOG.debug("Leader {} offerLeaderToFollowerMessage to receivingNodeId: {}, {} type: {}, zxid: 0x{}.",
                    sendingNodeId, receivingNodeId, receivingAddr, type, Long.toHexString(zxid));
            if (receivingNodeId == -1) {
                LOG.debug("The receiving node has crashed / un-exist!");
                return TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
            }
            switch (type) {
                case MessageType.LEADERINFO:
                    // LearnerHandler
                    followerLearnerHandlerMap.set(receivingNodeId, sendingSubnodeId);
                    leaderSyncFollowerCountMap.put(sendingNodeId, leaderSyncFollowerCountMap.get(sendingNodeId) - 1);
                    LOG.debug("leaderSyncFollowerCountMap, leader: {}, count: {}",
                            sendingNodeId, leaderSyncFollowerCountMap.get(sendingNodeId));
                    LOG.debug("-------leader {} is about to send LEADERINFO to follower {} !!!! will not be intercepted",
                            sendingNodeId, receivingNodeId);
                    break;
                case MessageType.DIFF:
                case MessageType.TRUNC:
                    syncTypeList.set(receivingNodeId, type);
                    followerLearnerHandlerSenderMap.set(receivingNodeId, sendingSubnodeId);
                    break;
                case MessageType.SNAP:
//                    // LearnerHandler
//                    followerLearnerHandlerMap.set(receivingNodeId, sendingSubnodeId);
//                    leaderSyncFollowerCountMap.put(sendingNodeId, leaderSyncFollowerCountMap.get(sendingNodeId) - 1);
                    nodePhases.set(sendingNodeId, Phase.SYNC);
                    nodePhases.set(receivingNodeId, Phase.SYNC);
                    syncTypeList.set(receivingNodeId, type);
                    break;
                case MessageType.NEWLEADER:
                    LOG.debug("-------leader {} is about to send NEWLEADER to follower {} !!!!",
                            sendingNodeId, receivingNodeId);
                    assert leaderSyncFollowerCountMap.containsKey(sendingNodeId);
                    // LearnerHandlerSender
                    followerLearnerHandlerSenderMap.set(receivingNodeId, sendingSubnodeId);
                    break;
                case MessageType.UPTODATE:
                    LOG.debug("-------leader {} is about to send UPTODATE to follower {} !!!!",
                            sendingNodeId, receivingNodeId);
                    LOG.debug("----set leader {} to BROADCAST phase !---", sendingNodeId);
                    nodePhases.set(sendingNodeId, Phase.BROADCAST);
                    followerLearnerHandlerSenderMap.set(receivingNodeId, sendingSubnodeId);
                    break;
                case MessageType.PROPOSAL:
                case MessageType.COMMIT:
                    // LearnerHandlerSender
                    followerLearnerHandlerSenderMap.set(receivingNodeId, sendingSubnodeId);
                    break;
                case TestingDef.MessageType.learnerHandlerReadRecord:
                    LOG.debug("-------leader {}'s learner handler {} (serving follower {}) is about to read record...",
                            sendingNodeId, sendingSubnodeId, receivingNodeId);
                    break;
            }

            //        // not in partition
//        // during client session establishment, do not intercept
//        if (!clientInitializationDone) {
//            LOG.debug("----client initialization is not done!---");
//            return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
//        }

            if (nodeStates.get(sendingNodeId).equals(NodeState.OFFLINE)
                    || nodeStates.get(sendingNodeId).equals(NodeState.STOPPING)) {
                return TestingDef.RetCode.NODE_CRASH;
            }

            if ( nodeStates.get(sendingNodeId).equals(NodeState.UNREADY)){
                LOG.debug("Will not intercept leader {} to follower {}'s type={} message as " +
                        "it is about to be looking...", sendingNodeId, receivingNodeId, type);
                return TestingDef.RetCode.BACK_TO_LOOKING;
            }

            // if partition occurs, just return without sending this message
            // mute the effect of events before adding it to the scheduling list
            if (partitionMap.get(sendingNodeId).get(receivingNodeId)){
                LOG.debug("Leader {} is trying to send a a type={} message to follower {}: " +
                        "where they are partitioned. Just return", sendingNodeId, type, receivingNodeId);
                return TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
            }


            switch (type) {
                case MessageType.LEADERINFO:
                    LOG.debug("-------leader {} is about to send LEADERINFO to follower {}!", sendingNodeId, receivingNodeId);
                    break;
                case MessageType.DIFF:
                    LOG.debug("-------leader {} is about to send DIFF to follower {}!", sendingNodeId, receivingNodeId);
                    break;
                case MessageType.TRUNC:
                    LOG.debug("-------leader {} is about to send TRUNC to follower {}!", sendingNodeId, receivingNodeId);
                    break;
                case MessageType.SNAP:
                    LOG.debug("-------leader {} is about to send SNAP to follower {}!", sendingNodeId, receivingNodeId);
                    break;
                case MessageType.COMMIT:
                    LOG.debug("-------leader {} is about to send COMMIT to follower {}!", sendingNodeId, receivingNodeId);
                    break;
                case MessageType.PROPOSAL:
                    LOG.debug("-------leader {} is about to send PROPOSAL to follower {}!", sendingNodeId, receivingNodeId);
                    break;
                case MessageType.NEWLEADER:
                    LOG.debug("-------leader {} is about to send NEWLEADER to follower {}!", sendingNodeId, receivingNodeId);
                    break;
                case MessageType.UPTODATE:
                    LOG.debug("-------leader {} is about to send UPTODATE to follower {}!", sendingNodeId, receivingNodeId);
                    break;
                case TestingDef.MessageType.learnerHandlerReadRecord:
                    if ( nodePhases.get(receivingNodeId).equals(Phase.SYNC)
                            || nodePhases.get(receivingNodeId).equals(Phase.BROADCAST)) {
                        LOG.debug("-------leader {}'s learner handler {} is about to read record from follower {}!",
                                sendingNodeId, sendingSubnodeId, receivingNodeId);
                        LOG.debug("before readRecordIntercepted: {}", readRecordIntercepted);
                        readRecordIntercepted.put(receivingNodeId, true);
                        LOG.debug("update readRecordIntercepted: {}", readRecordIntercepted);
                    } else {
                        LOG.debug("-------follower {} is still in discovery, will not intercept read record!", receivingNodeId);
                        return TestingDef.RetCode.NOT_INTERCEPTED;
                    }
                    break;
                default:
                    LOG.debug("-------leader is about to send a message that will not be intercepted!");
                    return TestingDef.RetCode.NOT_INTERCEPTED;
            }
        }



        // intercept the event
        final List<Event> predecessorEvents = new ArrayList<>();
        int id = generateEventId();
        final LeaderToFollowerMessageEvent messageEvent = new LeaderToFollowerMessageEvent(
                id, sendingSubnodeId, receivingNodeId, type, zxid, payload, leaderToFollowerMessageExecutor);
        messageEvent.addAllDirectPredecessors(predecessorEvents);

        synchronized (controlMonitor) {
            LOG.debug("Leader {} is offering a a type={} message to follower {}: msgId = {}, " +
                    "set subnode {} to SENDING state", sendingNodeId, type, receivingNodeId, id, sendingSubnodeId);

            addEvent(messageEvent);
            sendingSubnode.setState(SubnodeState.SENDING);

            controlMonitor.notifyAll();
            waitMessageReleased(id, sendingNodeId, sendingSubnodeId, messageEvent);

            if (messageEvent.getFlag() == TestingDef.RetCode.EXIT) {
                LOG.debug(" Test trace is over. Drop the event {}", messageEvent);
                if (sendingSubnode.getState().equals(SubnodeState.PROCESSING)) {
                    sendingSubnode.setState(SubnodeState.RECEIVING);
                }
                controlMonitor.notifyAll();
                return TestingDef.RetCode.EXIT;
            }

            if (type == TestingDef.MessageType.learnerHandlerReadRecord) {
                LOG.debug("readRecordIntercepted: {}", readRecordIntercepted);
                readRecordIntercepted.put(receivingNodeId, false);
                LOG.debug("after readRecordIntercepted: {}", readRecordIntercepted);
            }

            // normally, this event is released when scheduled except:
            // case 1: this event is released since the sending node is crashed
            if (NodeState.STOPPING.equals(nodeStates.get(sendingNodeId))) {
                LOG.debug("----------node {} is crashed! setting LeaderMessage executed and removed. {}",
                        sendingNodeId, messageEvent);
                id = TestingDef.RetCode.NODE_CRASH;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            // case 2: this event is released since the sending subnode is unregistered
            else if (SubnodeState.UNREGISTERED.equals(subnodes.get(sendingSubnodeId).getState())) {
                LOG.debug("----------subnode {} of node {} is unregistered! setting LeaderMessage executed and removed. {}",
                        sendingSubnode, sendingNodeId, messageEvent);
                id = TestingDef.RetCode.SUBNODE_UNREGISTERED;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            // case 3: this event is released when the network partition occurs
            else if (partitionMap.get(sendingNodeId).get(receivingNodeId) ||
                    messageEvent.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION) {
                LOG.debug("----------node {} and {} get partitioned! setting messageEvent executed and removed. {}",
                        sendingNodeId, receivingNodeId, messageEvent);
                sendingSubnode.setState(SubnodeState.RECEIVING);
                id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            else if (NodeState.UNREADY.equals(nodeStates.get(sendingNodeId))) {
                LOG.debug("----------node {} is about to become looking! setting LeaderMessage executed and removed. {}",
                        sendingNodeId, messageEvent);
                id = TestingDef.RetCode.BACK_TO_LOOKING;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }

//            if (type == MessageType.NEWLEADER && id > 0) {
//                LOG.debug("----set leader {} to BROADCAST phase !---", sendingNodeId);
//                nodePhases.set(sendingNodeId, Phase.BROADCAST);
//            }
            controlMonitor.notifyAll();
        }
        return id;
    }

    @Override
    public int offerLocalEvent(int subnodeId, SubnodeType subnodeType, long zxidOrEpoch, String payload, int type) throws RemoteException {
//        if (!subnodeType.equals(SubnodeType.QUORUM_PEER)) {
//            if (!clientInitializationDone) {
//                LOG.debug("----client initialization is not done!---");
//                return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
//            }
//        }

        final Subnode subnode = subnodes.get(subnodeId);
        final int nodeId = subnode.getNodeId();

        if (nodeStates.get(nodeId).equals(NodeState.OFFLINE) ||
                nodeStates.get(nodeId).equals(NodeState.STOPPING)) {
            return TestingDef.RetCode.NODE_CRASH;
        }

        if ( nodeStates.get(nodeId).equals(NodeState.UNREADY)){
            LOG.debug("Will not intercept node {}'s action: subnodeType={}, type={}, as " +
                    "it is about to be looking...", nodeId, subnodeType, type);
            return TestingDef.RetCode.BACK_TO_LOOKING;
        }

        int id = generateEventId();
        final LocalEvent localEvent =
                new LocalEvent(id, nodeId, subnodeId, subnodeType, payload, zxidOrEpoch, type, localEventExecutor);
        synchronized (controlMonitor) {
            LOG.debug("{} {} of Node {} is about to process the request ({}): msgId = {}, " +
                    "set subnode {} to SENDING state", subnodeType, subnodeId, nodeId, payload, id, subnodeId);
            addEvent(localEvent);
            subnode.setState(SubnodeState.SENDING);
            if (SubnodeType.COMMIT_PROCESSOR.equals(subnodeType)) {
                zxidToCommitMap.put(zxidOrEpoch, zxidToCommitMap.getOrDefault(zxidOrEpoch, 0) + 1);
            }
            controlMonitor.notifyAll();
            if (type == TestingDef.MessageType.leaderJudgingIsRunning) {
                waitMessageReleased(id, nodeId, subnodeId, localEvent);
            } else {
                waitMessageReleased(id, nodeId, subnodeId);
            }

            if (localEvent.getFlag() == TestingDef.RetCode.EXIT) {
                LOG.debug(" Test trace is over. Drop the event {}", localEvent);
                if (subnode.getState().equals(SubnodeState.PROCESSING)) {
                    subnode.setState(SubnodeState.RECEIVING);
                }
                controlMonitor.notifyAll();
                return TestingDef.RetCode.EXIT;
            }

            // case 1: this event is released since the node is crashed
            if (NodeState.STOPPING.equals(nodeStates.get(nodeId))) {
                LOG.debug("----------node {} is crashed! setting localEvent executed. {}", nodeId, localEvent);
                id = TestingDef.RetCode.NODE_CRASH;
                localEvent.setExecuted();
                schedulingStrategy.remove(localEvent);
            }
            // case 2: this event is released since the subnode is unregistered
            else if (SubnodeState.UNREGISTERED.equals(subnodes.get(subnodeId).getState())) {
                LOG.debug("----------subnode {} of node {} is unregistered! setting localEvent executed. {}",
                        subnodeId, nodeId, localEvent);
                id = TestingDef.RetCode.SUBNODE_UNREGISTERED;
                localEvent.setExecuted();
                schedulingStrategy.remove(localEvent);
            }// case 3: this event is released when the network partition occurs
            else if (localEvent.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION) {
                LOG.debug("---------get flag partitioned! setting leader-judging-isRunning event executed and removed. {}",
                        localEvent);
                subnode.setState(SubnodeState.RECEIVING);
                id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                localEvent.setExecuted();
                schedulingStrategy.remove(localEvent);
            }
            else if (NodeState.UNREADY.equals(nodeStates.get(nodeId))) {
                LOG.debug("---------node {} is about to become looking. NOT_INTERCEPTED. setting localEvent executed. {}", nodeId, localEvent);
                id = TestingDef.RetCode.BACK_TO_LOOKING;
                localEvent.setExecuted();
                schedulingStrategy.remove(localEvent);
            }
            controlMonitor.notifyAll();
        }
        return id;
    }


    @Override
    public int registerSubnode(final int nodeId, final SubnodeType subnodeType) throws RemoteException {
        final int subnodeId;
        synchronized (controlMonitor) {
            if (nodeStates.get(nodeId).equals(NodeState.OFFLINE) || nodeStates.get(nodeId).equals(NodeState.STOPPING)){
                LOG.debug("-----------will not register type: {} of node {},  since this node is {}--------",
                        subnodeType, nodeId, nodeStates.get(nodeId));
                return TestingDef.RetCode.NODE_CRASH;
            }
            if (nodeStates.get(nodeId).equals(NodeState.UNREADY)) {
                LOG.debug("-----------will not register type: {} of node {},  since this node is about to become looking:" +
                                " {}--------", subnodeType, nodeId, nodeStates.get(nodeId));
                return TestingDef.RetCode.BACK_TO_LOOKING;
            }
            if (nodePhases.get(nodeId).equals(Phase.ELECTION)) {
                switch (subnodeType) {
                    case COMMIT_PROCESSOR:
                    case SYNC_PROCESSOR:
                    case LEARNER_HANDLER:
                    case LEARNER_HANDLER_SENDER:
                        LOG.debug("-----------will not register previous type: {} of node {},  since this node is {}--------",
                                subnodeType, nodeId, nodeStates.get(nodeId));
                        return -1;
                }
            }
            subnodeId = subnodes.size();
            LOG.debug("-----------register subnode {} of node {}, type: {}--------", subnodeId, nodeId, subnodeType);
            final Subnode subnode = new Subnode(subnodeId, nodeId, subnodeType);
            subnodes.add(subnode);
            subnodeSets.get(nodeId).add(subnode);
            subnodeMap.put(subnodeId, nodeId);
        }
        return subnodeId;
    }

    @Override
    public void deregisterSubnode(final int subnodeId) throws RemoteException {
        synchronized (controlMonitor) {
            final Subnode subnode = subnodes.get(subnodeId);
            subnodeSets.get(subnode.getNodeId()).remove(subnode);
            if (!SubnodeState.UNREGISTERED.equals(subnode.getState())) {
                LOG.debug("---in deregisterSubnode, set {} {} of node {} UNREGISTERED",
                        subnode.getSubnodeType(), subnode.getId(), subnode.getNodeId());
                subnode.setState(SubnodeState.UNREGISTERED);
                // All subnodes may have become steady; give the scheduler a chance to make progress
                controlMonitor.notifyAll();
            }
        }
    }

    @Override
    public void registerFollowerSocketInfo(final int node, final String socketAddress) throws RemoteException {
        synchronized (controlMonitor) {
            followerSocketAddressBook.set(node, socketAddress);
            controlMonitor.notifyAll();
        }
    }

    @Override
    public void deregisterFollowerSocketInfo(int node) throws RemoteException {
        synchronized (controlMonitor) {
            followerSocketAddressBook.set(node, null);
            controlMonitor.notifyAll();
        }
    }

    @Override
    public void setProcessingState(final int subnodeId) throws RemoteException {
        synchronized (controlMonitor) {
            final Subnode subnode = subnodes.get(subnodeId);
            if (SubnodeState.RECEIVING.equals(subnode.getState())) {
                subnode.setState(SubnodeState.PROCESSING);
            }
        }
    }

    @Override
    public void setReceivingState(final int subnodeId) throws RemoteException {
        synchronized (controlMonitor) {
            final Subnode subnode = subnodes.get(subnodeId);
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                subnode.setState(SubnodeState.RECEIVING);
                controlMonitor.notifyAll();
            }
        }
    }

    @Override
    public void nodeOnline(final int nodeId) throws RemoteException {
        synchronized (controlMonitor) {
            LOG.debug("--------- set online {}", nodeId);
            nodeStates.set(nodeId, NodeState.ONLINE);
            nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
            controlMonitor.notifyAll();
        }
    }

    @Override
    public void nodeOffline(final int nodeId) throws RemoteException {
        synchronized (controlMonitor) {
            LOG.debug("--------- set offline {}", nodeId);
            nodeStates.set(nodeId, NodeState.OFFLINE);
            nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
            controlMonitor.notifyAll();
        }
    }

    /***
     * Called by the executor of node start event
     * @param nodeId
     * @throws RemoteException
     */
    public void startNode(final int nodeId) throws RemoteException {
        // 1. PRE_EXECUTION: set unstable state (set STARTING)
        nodeStates.set(nodeId, NodeState.STARTING);
        nodePhases.set(nodeId, Phase.ELECTION);
        nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
        votes.set(nodeId, null);
        leaderElectionStates.set(nodeId, LeaderElectionState.LOOKING);
        firstMessage.set(nodeId, null);
        followerSocketAddressBook.set(nodeId, null);
        followerLearnerHandlerMap.set(nodeId, null);
        followerLearnerHandlerSenderMap.set(nodeId, null);
        syncTypeList.set(nodeId, -1);


        // 2. EXECUTION
        ensemble.startNode(nodeId);
        // 3. POST_EXECUTION: wait for the state to be stable
        // the started node will call for remote service of nodeOnline(..)
        for (int id = 0; id < schedulerConfiguration.getNumNodes(); id++) {
            LOG.debug("--------nodeid: {}: phase: {}, leaderElectionState: {}",
                    id, nodePhases.get(id), leaderElectionStates.get(id));
        }
    }

    /***
     * Called by the executor of node crash event
     * @param nodeId
     */
    public void stopNode(final int nodeId) {
        // 1. PRE_EXECUTION: set unstable state (set STOPPING)
        LeaderElectionState role = leaderElectionStates.get(nodeId);
        LOG.debug("to stop node {}, {}", nodeId, role);
        int leaderId = leaderElectionStates.indexOf(LeaderElectionState.LEADING);
        boolean hasSending = false;
        for (final Subnode subnode : subnodeSets.get(nodeId)) {
            if (SubnodeState.SENDING.equals(subnode.getState())) {
                LOG.debug("----Node {} still has SENDING subnode {} {}: {}",
                        nodeId, subnode.getSubnodeType(), subnode.getId(), subnode.getState());
                hasSending = true;
                break;
            }
        }

        // IF there exists any threads about to send a message, then set the corresponding event executed
        if (hasSending) {
            // STOPPING state will make the pending message to be released immediately
            nodeStates.set(nodeId, NodeState.STOPPING);
            controlMonitor.notifyAll();
            // Wait util no node is STARTING or STOPPING.
            // This node will be set to OFFLINE by the last existing thread that release a sending message
            waitAllNodesSteady();
        }

        // Subnode management
        for (final Subnode subnode : subnodeSets.get(nodeId)) {
            subnode.setState(SubnodeState.UNREGISTERED);
        }
        subnodeSets.get(nodeId).clear();
        LOG.debug("-------Node {} 's subnodes has been cleared.", nodeId);

        // If hasSending == true, the node has been set OFFLINE when the last intercepted subnode is shutdown
        // o.w. set OFFLINE here anyway.
        nodeStates.set(nodeId, NodeState.OFFLINE);
        nodePhases.set(nodeId, Phase.NULL);
        nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
        for (int id = 0; id < schedulerConfiguration.getNumNodes(); id++) {
            LOG.debug("--------nodeid: {}: phase: {}", id, nodePhases.get(id));

            // node crash will not reset the partition
//            // recover the partition with this node
//            partitionMap.get(nodeId).set(id, false);
//            partitionMap.get(id).set(nodeId, false);
        }

        votes.set(nodeId, null);
        firstMessage.set(nodeId, false);
        // is it needed to still keep the role info before crash for further property check？
        leaderElectionStates.set(nodeId, LeaderElectionState.NULL);
        followerSocketAddressBook.set(nodeId, null);
        followerLearnerHandlerMap.set(nodeId, null);
        followerLearnerHandlerSenderMap.set(nodeId, null);
        syncTypeList.set(nodeId, -1);
        participants.remove(nodeId);

        // 2. EXECUTION
        ensemble.stopNode(nodeId);


        // 3. POST_EXECUTION: wait for the state to be stable
        if (role.equals(LeaderElectionState.FOLLOWING)) {
            // if a follower is crashed
            LOG.debug("participants: {}", participants);
            if (participants.size() <= schedulerConfiguration.getNumNodes() / 2) {
                // if leader loses quorum, then wait for the leader back into LOOKING
                if (leaderId >= 0 ) {
//                    participants.clear();
                    LOG.debug("Try to set flag NODE_PAIR_IN_PARTITION to leader's & crash node's events before the node get into LOOKING...");
                    // all participants: need to change node state
                    // drop message whose sender or receiver is crashed node or leader,
                    // which means leader's all message will be dropped
                    recordPartitionedEvent(new HashSet<Integer>() {{
                        add(nodeId);
                        add(leaderId);
                    }}, true);

                    LOG.debug("release Broadcast Events of leader {} and wait for it in LOOKING state", leaderId);
                    controlMonitor.notifyAll();
                    waitAliveNodesInLookingState(new HashSet<Integer>() {{
                        add(leaderId);
                    }});
                }
            } else {
                // leader: do not need to change node state
                // only drop the message between crashed node & leader, do not drop leader's message with other nodes
                recordPartitionedEvent(new HashSet<Integer>() {{
                    add(nodeId);
                    add(leaderId);
                }}, false);
                // TODO: release leader's corresponding learner handler thread
            }
        }
        else if (role.equals(LeaderElectionState.LEADING)) {
            // if leader un-exists, then wait for the other nodes back into LOOKING. For now we just wait.

            LOG.debug("Try to set flag NODE_PAIR_IN_PARTITION to all participants' events before the node get into LOOKING...");
//            recordCrashRelatedEventPartitioned(participants, true);
            for (int peer: participants) {
                LOG.debug("\n\n\n\n\nTry to set flag NODE_PAIR_IN_PARTITION to events between {} and {}...", nodeId, peer);
                recordPartitionedEvent(new HashSet<Integer>() {{
                    add(nodeId);
                    add(peer);
                }}, true);
            }
            // release all SYNC /COMMIT message
            LOG.debug("release Broadcast Events of server {} and wait for them in LOOKING state", participants);
            controlMonitor.notifyAll();
            waitAliveNodesInLookingState(new HashSet<>(participants));
        } else {
            // looking node crashed, other nodes do not need to transfer states
            recordCrashRelatedEventPartitioned(new HashSet<Integer>() {{
                add(nodeId);
            }},false);

        }
    }

    /***
     * drop the message between specific sender || receiver
     * @param peers
     * @param leaderShutdown whether leader is going to be looking or not
     * @return
     */
    public boolean recordCrashRelatedEventPartitioned(Set<Integer> peers, boolean leaderShutdown) {
        Set<Event> otherEvents = new HashSet<>();
        try {
            while (schedulingStrategy.hasNextEvent()) {
                final Event event = schedulingStrategy.nextEvent();
                if (event instanceof LeaderToFollowerMessageEvent) {
                    LeaderToFollowerMessageEvent e1 = (LeaderToFollowerMessageEvent) event;
                    final int sendingSubnodeId = e1.getSendingSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
                    final int sendingNodeId = sendingSubnode.getNodeId();
                    final int receivingNodeId = e1.getReceivingNodeId();
                    if ((peers.contains(sendingNodeId) || peers.contains(receivingNodeId))) {
                        LOG.debug("set flag NODE_PAIR_IN_PARTITION to event: {}", e1);
                        e1.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                        if (sendingSubnode.getState().equals(SubnodeState.SENDING)) {
                            sendingSubnode.setState(SubnodeState.PROCESSING);
                        }
                    }
                } else if (event instanceof FollowerToLeaderMessageEvent) {
                    FollowerToLeaderMessageEvent e1 = (FollowerToLeaderMessageEvent) event;
                    final int sendingSubnodeId = e1.getSendingSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
                    final int sendingNodeId = sendingSubnode.getNodeId();
                    final int receivingNodeId = e1.getReceivingNodeId();
                    if ((peers.contains(sendingNodeId) || peers.contains(receivingNodeId))) {
                        LOG.debug("set flag NODE_PAIR_IN_PARTITION to event: {}", e1);
                        e1.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                        if (sendingSubnode.getState().equals(SubnodeState.SENDING)) {
                            sendingSubnode.setState(SubnodeState.PROCESSING);
                        }
                    }
                } else if (event instanceof ElectionMessageEvent) {
                    ElectionMessageEvent e1 = (ElectionMessageEvent) event;
                    final int sendingSubnodeId = e1.getSendingSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
                    final int sendingNodeId = sendingSubnode.getNodeId();
                    final int receivingNodeId = e1.getReceivingNodeId();
                    if ((peers.contains(sendingNodeId) || peers.contains(receivingNodeId))) {
                        LOG.debug("set flag NODE_PAIR_IN_PARTITION to event: {}", e1);
                        e1.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                        if (sendingSubnode.getState().equals(SubnodeState.SENDING)) {
                            sendingSubnode.setState(SubnodeState.PROCESSING);
                        }
                    }
                } else if (leaderShutdown && event instanceof LocalEvent) {
                    LocalEvent e1 = (LocalEvent) event;
                    final int subnodeId = e1.getSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(subnodeId);
                    if (e1.getType() == TestingDef.MessageType.leaderJudgingIsRunning) {
                        LOG.debug("set flag NODE_PAIR_IN_PARTITION to event: {}", e1);
                        e1.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                        if (sendingSubnode.getState().equals(SubnodeState.SENDING)) {
                            sendingSubnode.setState(SubnodeState.PROCESSING);
                        }
                    }
                }
                otherEvents.add(event);
            }
            LOG.debug("Adding back all events during recording partition");
            for (Event e: otherEvents) {
                addEvent(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


    /***
     * drop the message between specific sender && receiver
     * @param peers
     * @param leaderShutdown whether leader is going to be looking or not
     * @return
     */
    public boolean recordPartitionedEvent(Set<Integer> peers, boolean leaderShutdown) {
        Set<Event> otherEvents = new HashSet<>();
        try {
            while (schedulingStrategy.hasNextEvent()) {
                final Event event = schedulingStrategy.nextEvent();
                if (event instanceof LeaderToFollowerMessageEvent) {
                    LeaderToFollowerMessageEvent e1 = (LeaderToFollowerMessageEvent) event;
                    final int sendingSubnodeId = e1.getSendingSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
                    final int sendingNodeId = sendingSubnode.getNodeId();
                    final int receivingNodeId = e1.getReceivingNodeId();
                    if (peers.contains(sendingNodeId) && peers.contains(receivingNodeId) && sendingNodeId != receivingNodeId) {
                        LOG.debug("set flag NODE_PAIR_IN_PARTITION to event: {}", e1);
                        e1.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                        if (sendingSubnode.getState().equals(SubnodeState.SENDING)) {
                            sendingSubnode.setState(SubnodeState.PROCESSING);
                        }
                    }
                } else if (event instanceof FollowerToLeaderMessageEvent) {
                    FollowerToLeaderMessageEvent e1 = (FollowerToLeaderMessageEvent) event;
                    final int sendingSubnodeId = e1.getSendingSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
                    final int sendingNodeId = sendingSubnode.getNodeId();
                    final int receivingNodeId = e1.getReceivingNodeId();
                    if (peers.contains(sendingNodeId) && peers.contains(receivingNodeId) && sendingNodeId != receivingNodeId) {
                        LOG.debug("set flag NODE_PAIR_IN_PARTITION to event: {}", e1);
                        e1.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                        if (sendingSubnode.getState().equals(SubnodeState.SENDING)) {
                            sendingSubnode.setState(SubnodeState.PROCESSING);
                        }
                    }
                } else if (event instanceof ElectionMessageEvent) {
                    ElectionMessageEvent e1 = (ElectionMessageEvent) event;
                    final int sendingSubnodeId = e1.getSendingSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
                    final int sendingNodeId = sendingSubnode.getNodeId();
                    final int receivingNodeId = e1.getReceivingNodeId();
                    if (peers.contains(sendingNodeId) && peers.contains(receivingNodeId) && sendingNodeId != receivingNodeId) {
                        LOG.debug("set flag NODE_PAIR_IN_PARTITION to event: {}", e1);
                        e1.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                        if (sendingSubnode.getState().equals(SubnodeState.SENDING)) {
                            sendingSubnode.setState(SubnodeState.PROCESSING);
                        }
                    }
                } else if (event instanceof LocalEvent) {
                    LocalEvent e1 = (LocalEvent) event;
                    final int nodeId = e1.getNodeId();
                    final int subnodeId = e1.getSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(subnodeId);
                    if (peers.contains(nodeId)) {
                        if (e1.getType() == TestingDef.MessageType.leaderJudgingIsRunning) {
                            if (leaderShutdown) {
                                LOG.debug("set flag NODE_PAIR_IN_PARTITION to event: {}", e1);
                                e1.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
                                if (sendingSubnode.getState().equals(SubnodeState.SENDING)) {
                                    sendingSubnode.setState(SubnodeState.PROCESSING);
                                }
                            }
                        }
//                        // TODO: set flag or not ?
//                        else if (e1.getType() == MessageType.ACKEPOCH) {
//                            LOG.debug("set flag EXIT to event: {}", e1);
//                            e1.setFlag(TestingDef.RetCode.EXIT);
//                        }
//                        else {
//                            LOG.debug("set flag NODE_PAIR_IN_PARTITION to event: {}", e1);
//                            e1.setFlag(TestingDef.RetCode.NODE_PAIR_IN_PARTITION);
//                            if (sendingSubnode.getState().equals(SubnodeState.SENDING)) {
//                                sendingSubnode.setState(SubnodeState.PROCESSING);
//                            }
//                        }
                    }

                }
                otherEvents.add(event);
            }
            LOG.debug("Adding back all events during recording partition");
            for (Event e: otherEvents) {
                addEvent(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


    /***
     *
     * @param peers
     * @param leaderShutdown whether leader is going to be looking or not
     * @return
     */
    public boolean releaseBroadcastEvent(Set<Integer> peers, boolean leaderShutdown) {
        Set<Event> otherEvents = new HashSet<>();
        try {
            while (schedulingStrategy.hasNextEvent()) {
                final Event event = schedulingStrategy.nextEvent();
                if (event instanceof LeaderToFollowerMessageEvent) {
                    LeaderToFollowerMessageEvent e1 = (LeaderToFollowerMessageEvent) event;
                    final int sendingSubnodeId = e1.getSendingSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
                    final int sendingNodeId = sendingSubnode.getNodeId();
                    final int receivingNodeId = e1.getReceivingNodeId();
                    if (peers.contains(sendingNodeId) || peers.contains(receivingNodeId)) {
                        e1.execute();
                    } else {
                        otherEvents.add(event);
                    }
                } else if (event instanceof FollowerToLeaderMessageEvent) {
                    FollowerToLeaderMessageEvent e1 = (FollowerToLeaderMessageEvent) event;
                    final int sendingSubnodeId = e1.getSendingSubnodeId();
                    final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
                    final int sendingNodeId = sendingSubnode.getNodeId();
                    final int receivingNodeId = e1.getReceivingNodeId();
                    if (peers.contains(sendingNodeId) || peers.contains(receivingNodeId) ) {
                        e1.execute();
                    } else {
                        otherEvents.add(event);
                    }
                } else
                if (event instanceof LocalEvent) {
                    LocalEvent e1 = (LocalEvent) event;
                    final int subnodeId = e1.getSubnodeId();
                    final Subnode subnode = subnodes.get(subnodeId);
                    final int nodeId = subnode.getNodeId();
                    if (peers.contains(nodeId)) {
                        if ( e1.getType() == TestingDef.MessageType.leaderJudgingIsRunning && (!leaderShutdown)) {
                            LOG.debug("leader is not going to shutdown. Do not release LeaderJudgingIsRunning event: {}", event);
                            otherEvents.add(event);
                        } else {
                            e1.setFlag(TestingDef.RetCode.NO_WAIT);
                            e1.execute();
                        }
                    } else {
                        otherEvents.add(event);
                    }
                } else {
                    otherEvents.add(event);
                }
            }

            for (Event e: otherEvents) {
                LOG.debug("Adding back event that is missed during making peers back to LOOKING: {}", e);
                addEvent(e);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }



    public int generateEventId() {
        return eventIdGenerator.incrementAndGet();
    }

    public int generateClientId() {
        return clientIdGenerator.incrementAndGet();
    }

    @Override
    public void updateVote(final int nodeId, final Vote vote) throws RemoteException {
        synchronized (controlMonitor) {
            votes.set(nodeId, vote);
            controlMonitor.notifyAll();
            if(vote == null){
                return;
            }
            try {
                executionWriter.write("Node " + nodeId + " final vote: " + vote.toString() + '\n');
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }
    }

    @Override
    public void initializeVote(int nodeId, Vote vote) throws RemoteException {
        synchronized (controlMonitor) {
            votes.set(nodeId, vote);
            controlMonitor.notifyAll();
        }
    }

    @Override
    public void updateLeaderElectionState(final int nodeId, final LeaderElectionState state) throws RemoteException {
        LOG.debug("before setting Node {} state: {}", nodeId, state);
        synchronized (controlMonitor) {
            leaderElectionStates.set(nodeId, state);
            if (LeaderElectionState.LOOKING.equals(state)) {
                nodePhases.set(nodeId, Phase.ELECTION);
                LOG.debug("set {}'s firstMessage null", nodeId);
                firstMessage.set(nodeId, null);
                votes.set(nodeId, null);
                followerSocketAddressBook.set(nodeId, null);
                followerLearnerHandlerMap.set(nodeId, null);
                followerLearnerHandlerSenderMap.set(nodeId, null);
                syncTypeList.set(nodeId, -1);
                participants.remove(nodeId);
            } else {
                nodePhases.set(nodeId, Phase.DISCOVERY);
                followerSocketAddressBook.set(nodeId, null);
                followerLearnerHandlerMap.set(nodeId, null);
                followerLearnerHandlerSenderMap.set(nodeId, null);
                syncTypeList.set(nodeId, -1);
            }
            try {
                LOG.debug("Writing execution file------Node {} state: {}", nodeId, state);
                executionWriter.write("\nNode " + nodeId + " state: " + state + '\n');
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
            for (int id = 0; id < schedulerConfiguration.getNumNodes(); id++) {
                LOG.debug("--------nodeid: {}: phase: {}", id, nodePhases.get(id));
            }
            controlMonitor.notifyAll();
        }
        LOG.debug("after setting Node {} state: {}", nodeId, state);
    }

    @Override
    public void initializeLeaderElectionState(int nodeId, LeaderElectionState state) throws RemoteException {
        synchronized (controlMonitor) {
            leaderElectionStates.set(nodeId, state);
            try {
                LOG.debug("Node {} initialized state: {}", nodeId, state);
                executionWriter.write("Node " + nodeId + " initialized state: " + state + '\n');
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }
    }

    @Override
    public void updateLastProcessedZxid(int nodeId, long lastProcessedZxid) throws RemoteException{
        synchronized (controlMonitor) {
            lastProcessedZxids.set(nodeId, lastProcessedZxid);
            nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);

            try {
                // Update allZxidRecords
                List<Long> zxidRecord = allZxidRecords.get(nodeId);
                LOG.debug("Node " + nodeId + "'s original record: {}", zxidRecord
                        .stream().map(p -> "0x" + Long.toHexString(p)).collect(Collectors.toList()));
                int len = zxidRecord.size();
                if (zxidRecord.get(len - 1) < lastProcessedZxid
                        && (lastProcessedZxid & 0xffffffffL) != 0L  ){
                    zxidRecord.add(lastProcessedZxid);
//                allZxidRecords.get(nodeId).add(lastProcessedZxid);
                    executionWriter.write(
                            "\n---Update Node " + nodeId + "'s last record: " + allZxidRecords.get(nodeId)
                                    .stream().map(p -> "0x" + Long.toHexString(p)).collect(Collectors.toList()));

                    // Update lastCommittedZxid by leader since in the test leader is always the first to update lastCommittedZxid
                    if (NodeState.ONLINE.equals(nodeStates.get(nodeId))
                            && LeaderElectionState.LEADING.equals(leaderElectionStates.get(nodeId))
                            && Phase.BROADCAST.equals(nodePhases.get(nodeId))) {
                        lastCommittedZxid.add(lastProcessedZxid);
                        executionWriter.write("\n---Update lastCommittedZxid " + lastCommittedZxid.stream()
                                .map(p -> "0x" + Long.toHexString(p)).collect(Collectors.toList()));
                    }
                }

                executionWriter.write(
                        "\n---Update Node " + nodeId + "'s lastProcessedZxid: 0x" + Long.toHexString(lastProcessedZxid));
                executionWriter.write(
                        "\n---All node's last record: " + allZxidRecords.stream()
                                .map(l -> l.stream().map(p -> "0x" + Long.toHexString(p)).collect(Collectors.toList()))
                                .collect(Collectors.toList()));
                executionWriter.write("\n");
                controlMonitor.notifyAll();
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }
    }

    @Override
    public int writeLongToFile(final int nodeId, final String name, final long epoch) throws RemoteException {
        synchronized (controlMonitor) {
            try {
                // Update currentEpoch / acceptedEpoch
                if (name.equals("currentEpoch")) {
                    currentEpochs.set(nodeId, epoch);
                } else if (name.equals("acceptedEpoch")) {
                    acceptedEpochs.set(nodeId, epoch);
                }
                LOG.debug("Node " + nodeId + "'s " + name + " file updated: " + Long.toHexString(epoch));
                executionWriter.write(
                        "\n---Update Node " + nodeId + "'s " + name + " file: " + Long.toHexString(epoch));
                executionWriter.write("\n");
                controlMonitor.notifyAll();
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }
        if (name.equals("currentEpoch")) {
            switch (leaderElectionStates.get(nodeId)) {
                case LEADING:
                    LOG.debug("Leader updates its currentEpoch file!");
                    assert nodePhases.get(nodeId).equals(Phase.SYNC);
//                    return offerLocalEvent(getSubnodeId(nodeId, SubnodeType.QUORUM_PEER),
//                            SubnodeType.QUORUM_PEER,
//                            epoch, null, TestingDef.MessageType.LEADERINFO);
                    break;
                case FOLLOWING:
                    assert nodePhases.get(nodeId).equals(Phase.SYNC);
                    return offerLocalEvent(getSubnodeId(nodeId, SubnodeType.QUORUM_PEER),
                            SubnodeType.QUORUM_PEER,
                            epoch, null, TestingDef.MessageType.NEWLEADER);
                default:
                    LOG.debug("Non-leader/follower updates its currentEpoch file!");
            }
        }
        return TestingDef.RetCode.NOT_INTERCEPTED;
    }

    public void recordProperties(final int step, final long startTime, final Event event) throws IOException {
        executionWriter.write("\n---Step: " + step + "--->");
        executionWriter.write(event.toString());
        executionWriter.write("\nlastProcessedZxid: 0x");
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
            executionWriter.write(Long.toHexString(lastProcessedZxids.get(nodeId)) + " # ");
        }
        executionWriter.write("\ncurrentEpoch: 0x");
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
            executionWriter.write(Long.toHexString(currentEpochs.get(nodeId)) + " # ");
        }
        executionWriter.write("\nacceptedEpoch: 0x");
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
            executionWriter.write(Long.toHexString(acceptedEpochs.get(nodeId)) + " # ");
        }
        executionWriter.write("\ntime/ms: " + (System.currentTimeMillis() - startTime) + "\n");
        executionWriter.flush();
    }

    public void updateResponseForClientRequest(ClientRequestEvent event) throws IOException {
        executionWriter.write("\n---Get response of " + event.getType() + " (Async for setData): ");
        executionWriter.write(event.toString() + "\n");
        executionWriter.flush();
    }

    @Override
    public void readyForBroadcast(int subnodeId) throws RemoteException {
        // TODO: Leader needs to collect quorum. Here we suppose 1 learnerHanlder is enough for 3-node ensemble
        final Subnode subnode = subnodes.get(subnodeId);
        final int nodeId = subnode.getNodeId();
        LOG.debug("Node {} is ready to broadcast", nodeId);
        synchronized (controlMonitor) {
            nodePhases.set(nodeId, Phase.BROADCAST);
            controlMonitor.notifyAll();

            for (int id = 0; id < schedulerConfiguration.getNumNodes(); id++) {
                LOG.debug("--------nodeid: {}: phase: {}", id, nodePhases.get(id));
            }
        }
    }


    /***
     * The following predicates are general to some type of events.
     * Should be called while holding a lock on controlMonitor
     */

    /***
     * wait for client session initialization finished
     */
    public void waitClientSessionReady(final int clientId) {
        final WaitPredicate clientSessionReady = new ClientSessionReady(this, clientId);
        wait(clientSessionReady, 0L);
    }

    public void waitPrimeConnectionDone(final int clientId) {
        final WaitPredicate primeConnectionDone = new PrimeConnectionDone(this, clientId);
        wait(primeConnectionDone, 0L);
    }

    /***
     * General pre-/post-condition
     */
    private final WaitPredicate allNodesSteady = new AllNodesSteady(this);
    public void waitAllNodesSteady() {
        wait(allNodesSteady, 0L);
    }

    /***
     * Pre-condition for election vote property check
     */
    private final WaitPredicate allNodesVoted = new AllNodesVoted(this);
    // Should be called while holding a lock on controlMonitor
    private boolean waitAllNodesVoted() {
        wait(allNodesVoted, 100L);
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
            if (LeaderElectionState.LEADING.equals(leaderElectionStates.get(nodeId))) {
                return true;
            }
        }
        LOG.debug("Leader does not exist after voting!");
        return false;
    }

    /***
     * Pre-condition for election vote property check for particular peers
     */
    // Should be called while holding a lock on controlMonitor
    private boolean waitAllParticipantsVoted(final Set<Integer> participants) {
        final WaitPredicate allParticipantsVoted = new AllNodesVoted(this, participants);
        wait(allParticipantsVoted, 0L);
        boolean leaderExist = false;
        boolean lookingExist = false;
        for (int nodeId: participants) {
            if (LeaderElectionState.LEADING.equals(leaderElectionStates.get(nodeId))) {
                leaderExist = true;
            } else if (!LeaderElectionState.FOLLOWING.equals(leaderElectionStates.get(nodeId))) {
                lookingExist = true;
            }
        }
        if (!leaderExist) {
            LOG.debug("Leader does not exist!");
        } else if (lookingExist) {
            LOG.debug("Some node still looking after voting!");
        } else {
            LOG.debug("All participants have voted!");
        }
        return leaderExist && !lookingExist;
    }

    /**
     * Pre-condition for scheduling an internal event
     *
     * @param strategy
     * @param action
     * @param nodeId processingNodeId
     * @param peerId sendingNodeId
     * @param modelZxid
     * @return
     */
    private Event waitTargetInternalEventReady(ExternalModelStrategy strategy,
                                               ModelAction action,
                                               Integer nodeId,
                                               Integer peerId,
                                               long modelZxid) {
        final TargetInternalEventReady targetInternalEventReady =
                new TargetInternalEventReady(this, strategy, action, nodeId, peerId, modelZxid);
        wait(targetInternalEventReady, 1000L);
        Event e = targetInternalEventReady.getEvent();
        return e;
    }


    private void waitAllNodesVoted(final long timeout) {
        wait(allNodesVoted, timeout);
    }

    /***
     * Post-condition for election & Pre-condition for LeaderSyncFollowers
     */
    private void waitLeaderSyncReady(final int leaderId, List<Integer> peers) {
        WaitPredicate leaderSyncReady = new LeaderSyncReady(this, leaderId, peers);
        wait(leaderSyncReady, 0L);
    }

    /***
     * Pre-condition for client requests.
     * Note: session creation is all a type of client request, so this is better placed before client session creation.
     */
    private final WaitPredicate allNodesSteadyBeforeRequest = new AllNodesSteadyBeforeRequest(this);
    // Should be called while holding a lock on controlMonitor
    public void waitAllNodesSteadyBeforeRequest() {
        wait(allNodesSteadyBeforeRequest, 0L);
    }

    /***
     * Post-condition for a follower process UPTODATE.
     * Note: session creation is all a type of client request, so this is better placed before client session creation.
     */
    // Should be called while holding a lock on controlMonitor
    public void waitFollowerSteadyAfterProcessingUPTODATE(final int followerId) {
        final WaitPredicate followerSteadyAfterProcessingUPTODATE =
                new FollowerSteadyAfterProcessingUPTODATE(this, followerId);
        wait(followerSteadyAfterProcessingUPTODATE, 0L);
    }

    /***
     * Pre-condition for client mutation
     */
    private final WaitPredicate allNodesSteadyBeforeMutation = new AllNodesSteadyBeforeMutation(this);
    @Deprecated
    public void waitAllNodesSteadyBeforeMutation() {
        wait(allNodesSteadyBeforeMutation, 0L);
    }

    /***
     * Post-condition for client mutation
     */
    private final WaitPredicate allNodesSteadyAfterMutation = new AllNodesSteadyAfterMutation(this);
    public void waitAllNodesSteadyAfterMutation() {wait(allNodesSteadyAfterMutation, 0L);}


    /***
     * Pre-condition for logging
     */
    private final WaitPredicate allNodesLogSyncSteady = new AllNodesLogSyncSteady(this);
    public void waitAllNodesLogSyncSteady() {
        wait(allNodesLogSyncSteady, 0L);
    }

    public void waitAllNodesSteadyAfterQuorumSynced() {
        final WaitPredicate allNodesSteadyAfterQuorumSynced = new AllNodesSteadyAfterQuorumSynced(this, participants);
        wait(allNodesSteadyAfterQuorumSynced, 0L);
    }


    /**
     * The following predicates are specific to an event.
     */


    public void waitResponseForClientRequest(ClientRequestEvent event) {
        final WaitPredicate responseForClientRequest = new ResponseForClientRequest(this, event);
        wait(responseForClientRequest, 0L);
    }

    /***
     * Pre-condition for scheduling the first event in the election
     * @param nodeId
     */
    private void waitFirstMessageOffered(final int nodeId) {
        final WaitPredicate firstMessageOffered = new FirstMessageOffered(this, nodeId);
        wait(firstMessageOffered, 0L);
    }

    private void waitNewMessageOffered() {
        final WaitPredicate newMessageOffered = new NewMessageOffered(this);
        wait(newMessageOffered, 0L);
    }

    private void waitClientSessionClosed(final int clientId) {
        final WaitPredicate clientSessionClosed = new ClientSessionClosed(this, clientId);
        wait(clientSessionClosed, 0L);
    }

    public void waitClientRequestOffered(final int clientId) {
        final WaitPredicate clientRequestOffered = new ClientRequestOffered(this, clientId);
        wait(clientRequestOffered, 1000L);
    }

    /***
     * Pre-condition for scheduling the message event
     * Including: notification message in the election, leader-to-follower message, log message
     * @param msgId
     * @param sendingNodeId
     */
    private void waitMessageReleased(final int msgId, final int sendingNodeId) {
        final WaitPredicate messageReleased = new MessageReleased(this, msgId, sendingNodeId);
        wait(messageReleased, 0L);
    }

    /***
     * For local event
     * @param msgId
     * @param sendingNodeId
     * @param sendingSubnodeId
     */
    private void waitMessageReleased(final int msgId, final int sendingNodeId, final int sendingSubnodeId) {
        final WaitPredicate messageReleased = new MessageReleased(this, msgId, sendingNodeId, sendingSubnodeId);
        wait(messageReleased, 0L);
    }

    /***
     * For message event that will be affected by partition
     * @param msgId
     * @param sendingNodeId
     * @param sendingSubnodeId
     * @param event
     */
    private void waitMessageReleased(final int msgId, final int sendingNodeId, final int sendingSubnodeId, final Event event) {
        final WaitPredicate messageReleased = new MessageReleased(this, msgId, sendingNodeId, sendingSubnodeId, event);
        wait(messageReleased, 0L);
    }

    /***
     * Pre-condition for scheduling the log message event
     * @param msgId
     * @param syncNodeId
     */
    private void waitLogRequestReleased(final int msgId, final int syncNodeId) {
        final WaitPredicate logRequestReleased = new LogRequestReleased(this, msgId, syncNodeId);
        wait(logRequestReleased, 0L);
    }

    /***
     * Post-condition for scheduling the commit message event
     * @param msgId
     * @param nodeId
     */
    public void waitCommitProcessorDone(final int msgId, final int nodeId) {
        final long zxid = lastProcessedZxids.get(nodeId);
        final WaitPredicate commitProcessorDone = new CommitProcessorDone(this, msgId, nodeId, zxid);
        wait(commitProcessorDone, 0L);
    }

    /***
     * Attention!!!
     * Post-condition for scheduling LeaderSyncFollower only
     * Forced
     * @param nodeId
     */
    public void waitCurrentEpochUpdated(final int nodeId, final long acceptedEpoch) {
        final WaitPredicate currentEpochFileUpdated = new CurrentEpochFileUpdated(this, nodeId, acceptedEpoch);
        wait(currentEpochFileUpdated, 0L);
    }

    /***
     * Note: this is used when learnerHandler's COMMIT message is not targeted.
     * o.w. use global predicate AllNodesSteadyAfterQuorumSynced
     * Post-condition for scheduling quorum log message events
     */
    public void waitQuorumToCommit(final LocalEvent event) {
        final long zxid = event.getZxid();
        final int nodeNum = schedulerConfiguration.getNumNodes();
        final WaitPredicate quorumToCommit = new QuorumToCommit(this, zxid, nodeNum);
        wait(quorumToCommit, 0L);
    }

    /***
     * Pre-condition for scheduling the leader-to-follower message event
     * @param addr
     */
    private void waitFollowerSocketAddrRegistered(final String addr) {
        final WaitPredicate followerSocketAddrRegistered = new FollowerSocketAddrRegistered(this, addr);
        wait(followerSocketAddrRegistered, 0L);
    }

    private void waitAliveNodesInLookingState() {
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
            nodeStates.set(nodeId, NodeState.UNREADY);
        }
        final WaitPredicate aliveNodesInLookingState = new AliveNodesInLookingState(this);
        wait(aliveNodesInLookingState, 0L);
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
            nodeStates.set(nodeId, NodeState.ONLINE);
        }
    }

    public void waitAliveNodesInLookingState(final Set<Integer> peers) {
        for (int peer: peers) {
            nodeStates.set(peer, NodeState.UNREADY);
        }
        final WaitPredicate aliveNodesInLookingState = new AliveNodesInLookingState(this, peers);
        wait(aliveNodesInLookingState, 0L);
        for (int peer: peers) {
            nodeStates.set(peer, NodeState.ONLINE);
        }
    }

    public void waitAliveNodesInLookingState(final Set<Integer> peers, final long timeout) {
        for (int peer: peers) {
            nodeStates.set(peer, NodeState.UNREADY);
        }
        final WaitPredicate aliveNodesInLookingState = new AliveNodesInLookingState(this, peers);
        wait(aliveNodesInLookingState, timeout);
        for (int peer: peers) {
            nodeStates.set(peer, NodeState.ONLINE);
        }
    }

    public void waitSyncTypeDetermined(final int nodeId) {
        final WaitPredicate syncTypeDetermined = new SyncTypeDetermined(this, nodeId);
        wait(syncTypeDetermined, 0L);
    }

    public void waitFollowerMappingLearnerHandlerSender(final int nodeId) {
        final WaitPredicate followerMappingLearnerHandlerSender = new FollowerMappingLearnerHandlerSender(this, nodeId);
        wait(followerMappingLearnerHandlerSender, 0L);
    }

    /***
     * Post-condition for specific subnode in SENDING state
     */
    public void waitSubnodeInSendingState(final int subnodeId) {
        final WaitPredicate subnodeInSendingState = new SubnodeInSendingState(this, subnodeId);
        wait(subnodeInSendingState, 0L);
    }

    public void waitSubnodeTypeSending(int nodeId, SubnodeType subnodeType) {
        int subnodeId = getSubnodeId(nodeId, subnodeType);
        if (subnodeId >= 0) {
            waitSubnodeInSendingState(subnodeId);
        }
    }

    private void wait(final WaitPredicate predicate, final long timeout) {
        LOG.debug("Waiting for {}\n\n\n", predicate.describe());
        final long startTime = System.currentTimeMillis();
        long endTime = startTime;
        while (!predicate.isTrue() && (timeout == 0L || endTime - startTime < timeout)) {
            try {
                if (timeout == 0L) {
                    controlMonitor.wait();
                } else {
                    controlMonitor.wait(Math.max(1L, timeout - (endTime - startTime)));
                }
            } catch (final InterruptedException e) {
                LOG.debug("Interrupted from waiting on the control monitor");
            } finally {
                endTime = System.currentTimeMillis();
            }
        }
        LOG.debug("Done waiting for {}\n\n\n\n\n", predicate.describe());
    }

    public int nodIdOfSubNode(int subNodeID){
        return subnodeMap.get(subNodeID);
    }

    private int getSubnodeId(int nodeId, SubnodeType subnodeType) {
        Set<Subnode> subnodes = subnodeSets.get(nodeId);
        for (final Subnode subnode : subnodes) {
            if (subnode.getSubnodeType().equals(subnodeType)) {
                LOG.debug("------node {}'s subnode: {}, {}, {}",
                        nodeId, subnode.getId(), subnode.getSubnodeType(), subnode.getState());
//                // set the receiving QUORUM_PEER subnode to be PROCESSING
//                subnode.setState(SubnodeState.PROCESSING);
                return subnode.getId();
            }
        }
        return -1;
    }
}