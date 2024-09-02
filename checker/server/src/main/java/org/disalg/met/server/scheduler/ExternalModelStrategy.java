package org.disalg.met.server.scheduler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.disalg.met.api.*;
import org.disalg.met.api.configuration.SchedulerConfigurationException;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.Event;
import org.disalg.met.server.event.FollowerToLeaderMessageEvent;
import org.disalg.met.server.event.LeaderToFollowerMessageEvent;
import org.disalg.met.server.event.LocalEvent;
import org.disalg.met.server.state.Subnode;
import org.disalg.met.server.statistics.ExternalModelStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ExternalModelStrategy implements SchedulingStrategy{
    private static final Logger LOG = LoggerFactory.getLogger(ExternalModelStrategy.class);

    private final TestingService testingService;

    private final Random random;

    private File dir;
    private File[] files;
    private List<Trace> traces = new LinkedList<>();
    private int count = 0;
    private Trace currentTrace = null;

    private boolean nextEventPrepared = false;
    private Event nextEvent = null;
    private final Set<Event> events = new HashSet<>();

    private final ExternalModelStatistics statistics;

    public ExternalModelStrategy(TestingService testingService, Random random, File dir, final ExternalModelStatistics statistics) throws SchedulerConfigurationException {
        this.testingService = testingService;
        this.random = random;
        this.dir = dir;
        this.files = new File(String.valueOf(dir)).listFiles();
        assert files != null;
        this.statistics = statistics;
        load();
    }

    public int getTracesNum() {
        return count;
    }

    public Trace getCurrentTrace(final int idx) {
        assert idx < count;
        currentTrace = traces.get(idx);
        return currentTrace;
    }

    public Set<Event> getEvents() {
        return events;
    }

    public void clearEvents() {
        events.clear();
    }

    @Override
    public void add(final Event event) {
        LOG.debug("Adding event: {}", event.toString());
        events.add(event);
        if (nextEventPrepared && nextEvent == null) {
            nextEventPrepared = false;
        }
    }

    @Override
    public void remove(Event event) {
        LOG.debug("Removing event: {}", event.toString());
        events.remove(event);
        if (nextEventPrepared) {
            nextEventPrepared = false;
        }
    }

    @Override
    public boolean hasNextEvent() {
        if (!nextEventPrepared) {
            try {
                prepareNextEvent();
            } catch (SchedulerConfigurationException e) {
                LOG.error("Error while preparing next event from trace {}", currentTrace);
                e.printStackTrace();
            }
        }
        return nextEvent != null;
    }

    @Override
    public Event nextEvent() {
        if (!nextEventPrepared) {
            try {
                prepareNextEvent();
            } catch (SchedulerConfigurationException e) {
                LOG.error("Error while preparing next event from trace {}", currentTrace);
                e.printStackTrace();
                return null;
            }
        }
        nextEventPrepared = false;
        LOG.debug("nextEvent: {}", nextEvent.toString());
        return nextEvent;
    }

    private void prepareNextEvent() throws SchedulerConfigurationException {
        final List<Event> enabled = new ArrayList<>();
        LOG.debug("prepareNextEvent: events.size: {}", events.size());
        for (final Event event : events) {
            if (event.isEnabled()) {
                LOG.debug("enabled : {}", event.toString());
                enabled.add(event);
            }
        }
        statistics.reportNumberOfEnabledEvents(enabled.size());

        nextEvent = null;
        if (enabled.size() > 0) {
            final int i = random.nextInt(enabled.size());
            nextEvent = enabled.get(i);
            events.remove(nextEvent);
        }
        nextEventPrepared = true;
    }

    public void load() throws SchedulerConfigurationException {
        LOG.debug("Loading traces from files");
        try {
            for (File file : files) {
                if (file.isFile() && file.exists()) {
                    Trace trace = importTrace(file);
                    if (null == trace) continue;
                    traces.add(trace);
                    count++;
                } else {
                    LOG.debug("file does not exists! ");
                }
            }
            assert count == traces.size();
        } catch (final IOException e) {
            LOG.error("Error while loading execution data from {}", dir);
            throw new SchedulerConfigurationException(e);
        }
    }

    public Trace importTrace(File file) throws IOException {
        String filename = file.getName();
        // Only json files will be parsed
        if( !filename.endsWith(".json") || filename.startsWith(".") ) {
            return null;
        }
        LOG.debug("Importing trace from file {}", filename);

        // acquire file text
        InputStreamReader read = null;
        try {
            read = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert read != null;
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt;
        StringBuffer sb = new StringBuffer();
        while ((lineTxt = bufferedReader.readLine()) != null) {
            sb.append(lineTxt);
        }
        read.close();

        // parse json & store to the EventSequence structure
        String fileTxt = sb.toString().replaceAll("\r\n", "");
        JSONArray stateSeq = new JSONArray();
        if (StringUtils.isNoneBlank(fileTxt)) {
            stateSeq = JSONArray.parseArray(fileTxt);
        }

        // get cluster info
        JSONObject metadata = (JSONObject) stateSeq.remove(0);
        ModelVersion modelVersion = metadata.containsKey("version") ?
                ModelVersion.valueOf(metadata.getString("version")) : ModelVersion.DEFAULT;
        int serverNum = (int) metadata.get("server_num");
        List<String> serverIds = (List<String>) metadata.get("server_id");
        LOG.debug("modelVersion: {}, serverNum: {}, serverId: {}, eventCount: {}",
                modelVersion, serverNum, serverIds, stateSeq.size());

        Trace trace = new Trace(filename, modelVersion, serverNum, serverIds, stateSeq);
        summarizeTrace(stateSeq);

        return trace;
    }

    void summarizeTrace(JSONArray stateSeq) {
        Set<String> keys = new HashSet<>();
        List<String> events = new LinkedList<>();
        for (Object o : stateSeq) {
            JSONObject jsonObject = (JSONObject) o;
            Iterator<String> keyIterator = jsonObject.keySet().iterator();
            String key = keyIterator.next();
            String action = key.equals("Step") ? keyIterator.next() : key;
            keys.add(action);
            events.add(action);
        }
        LOG.debug("keySize: {}, keys: {}", keys.size(), keys);
        LOG.debug("eventCount: {}, events: {}", events.size(), events);
    }

    public Event getNextInternalEvent(ModelAction action, int processingNodeId, int sendingNodeId, long modelZxid) throws SchedulerConfigurationException {
        // 1. get all enabled events
        final List<Event> enabled = new ArrayList<>();
        LOG.debug("prepareNextEvent: events.size: {}", events.size());
        for (final Event event : events) {
            if (event.isEnabled()) {
                LOG.debug("enabled : {}", event.toString());
                enabled.add(event);
            }
        }
        statistics.reportNumberOfEnabledEvents(enabled.size());

        if (enabled.size() == 0) {
            throw new SchedulerConfigurationException();
        }
        nextEvent = null;

        // 2. search specific pre-condition event that should be lied in the sender
        LOG.debug(">> Try to schedule action:{}, processingNodeId: {}, sendingNodeId: {}, modelZxid: {} ",
                action, processingNodeId, sendingNodeId, Long.toHexString(modelZxid));
        switch (action) {
            case LeaderProcessACKEPOCH: // follower about to release ACKEPOCH
            case LeaderProcessACKLD: // follower about to release ACKLD
            case FollowerToLeaderACK: // follower about to release ACK
                searchFollowerMessage(action, sendingNodeId, processingNodeId, modelZxid, enabled);
                break;
            case FollowerProcessLEADERINFO: // leader (learner handler) about to release LEADERINFO
            case FollowerProcessDIFF: // leader (learner handler) about to release DIFF
            case FollowerProcessTRUNC: // leader (learner handler) about to release TRUNC
            case FollowerProcessSNAP: // leader (learner handler) about to release SNAP
            case FollowerProcessPROPOSALInSync: // leader (learner handler) about to release PROPOSAL
            case FollowerProcessCOMMITInSync: // leader (learner handler) about to release COMMIT
            case FollowerProcessNEWLEADER: // leader (learner handler) about to release NEWLEADER
            case LearnerHandlerReadRecord:
            case FollowerProcessUPTODATE: // leader (learner handler) about to release UPTODATE
            case LeaderToFollowerProposal: // leader (learner handler) about to release PROPOSAL
            case LeaderToFollowerCOMMIT: // leader (learner handler) about to release COMMIT
                searchLeaderMessage(action, sendingNodeId, processingNodeId, modelZxid, enabled);
                break;
            case LeaderSyncFollower: // leader (learner handler) about to proceed after current epoch updated
            case FollowerLogRequestWhenProcessingNEWLEADER: // follower about to proceed after current epoch updated
            case LeaderLog: // leader (learner handler) about to log
            case FollowerLog: // follower about to log
            case LeaderCommit: // leader (learner handler) about to commit
            case FollowerCommit: // follower about to commit
                searchLocalMessage(action, sendingNodeId, processingNodeId, modelZxid, enabled);
                break;
        }

        if ( nextEvent != null){
            LOG.debug("next event exists! {}", nextEvent);
        } else {
            throw new SchedulerConfigurationException();
        }

        nextEventPrepared = false;
        return nextEvent;
    }

    public void searchLeaderMessage(final ModelAction action,
                                    final int leaderId,
                                    final int followerId,
                                    final long modelZxid,
                                    List<Event> enabled) {
        for (final Event e : enabled) {
            if (e instanceof LeaderToFollowerMessageEvent) {
                final LeaderToFollowerMessageEvent event = (LeaderToFollowerMessageEvent) e;
                final int receivingNodeId = event.getReceivingNodeId();
                final int sendingSubnodeId = event.getSendingSubnodeId();
                final Subnode sendingSubnode = testingService.getSubnodes().get(sendingSubnodeId);
                final int sendingNodeId = sendingSubnode.getNodeId();
                if (sendingNodeId != leaderId || receivingNodeId != followerId) continue;
                final int type = event.getType();
                switch (type) {
                    case MessageType.LEADERINFO:
                        if (!action.equals(ModelAction.FollowerProcessLEADERINFO)) continue;
                        if (modelZxid != event.getZxid()) {
                            LOG.warn("LEADERINFO message: model zxid: {} != event zxid : {}", modelZxid, event.getZxid());
                            continue;
                        }
                        break;
                    case MessageType.DIFF:
                        if (!action.equals(ModelAction.FollowerProcessDIFF)) continue;
                        if (!checkZxidConformance(modelZxid, event.getZxid())) continue;
                        break;
                    case MessageType.TRUNC:
                        if (!action.equals(ModelAction.FollowerProcessTRUNC)) continue;
                        if (!checkZxidConformance(modelZxid, event.getZxid())) continue;
                        break;
                    case MessageType.SNAP:
                        if (!action.equals(ModelAction.FollowerProcessSNAP)) continue;
                        if (!checkZxidConformance(modelZxid, event.getZxid())) continue;
                        break;
                    case MessageType.PROPOSAL:
                        if (testingService.getNodePhases().get(followerId).equals(Phase.SYNC)) {
                            if (!action.equals(ModelAction.FollowerProcessPROPOSALInSync)) continue;
                        } else {
                            if (!action.equals(ModelAction.LeaderToFollowerProposal)) continue;
                        }
                        if (!checkZxidConformance(modelZxid, event.getZxid())) continue;
                        break;
                    case MessageType.COMMIT:
                        if (testingService.getNodePhases().get(followerId).equals(Phase.SYNC)) {
                            if (!action.equals(ModelAction.FollowerProcessCOMMITInSync)) continue;
                        } else {
                            if (!action.equals(ModelAction.LeaderToFollowerCOMMIT)) continue;
                        }
                        if (!checkZxidConformance(modelZxid, event.getZxid())) continue;
                        break;
                    case MessageType.NEWLEADER:
                        if (!action.equals(ModelAction.FollowerProcessNEWLEADER)) continue;
                        break;
                    case MessageType.UPTODATE:
                        if (!action.equals(ModelAction.FollowerProcessUPTODATE)) continue;
                        break;
                    case TestingDef.MessageType.learnerHandlerReadRecord:
                        if (!action.equals(ModelAction.LearnerHandlerReadRecord)) continue;
                        break;
                    default:
                        continue;
                }
                nextEvent = event;
                events.remove(nextEvent);
                break;
            }
        }
    }

    public void searchFollowerMessage(final ModelAction action,
                                      final int followerId,
                                      final int leaderId,
                                      final long modelZxid,
                                      List<Event> enabled) {
        for (final Event e : enabled) {
            if (e instanceof FollowerToLeaderMessageEvent) {
                final FollowerToLeaderMessageEvent event = (FollowerToLeaderMessageEvent) e;
                final int receivingNodeId = event.getReceivingNodeId();
                final int sendingSubnodeId = event.getSendingSubnodeId();
                final Subnode sendingSubnode = testingService.getSubnodes().get(sendingSubnodeId);
                final int sendingNodeId = sendingSubnode.getNodeId();
                if (sendingNodeId != followerId || receivingNodeId != leaderId) continue;
                final int lastReadType = event.getType(); // Note: this describes leader's previous message type that this ACK replies to
                switch (lastReadType) {
                    case MessageType.LEADERINFO:
                        if (!action.equals(ModelAction.LeaderProcessACKEPOCH)) continue;
                        break;
                    case MessageType.NEWLEADER:
                        if (!action.equals(ModelAction.LeaderProcessACKLD)) continue;
                        break;
                    case MessageType.UPTODATE:
                        if (!action.equals(ModelAction.FollowerToLeaderACK)) continue;
                        if (modelZxid != 0L) continue;
                        break;
                    case MessageType.PROPOSAL: // as for ACK to PROPOSAL during SYNC, we regard it as a local event
                    case MessageType.PROPOSAL_IN_SYNC:
                        if (!action.equals(ModelAction.FollowerToLeaderACK) ) continue;
                        if (modelZxid == 0L) continue;
                        if (!checkZxidConformance(modelZxid, event.getZxid())) continue;
                        break;
                    default:
                        continue;
                }
                nextEvent = event;
                events.remove(nextEvent);
                break;
            }
        }
    }

    public void searchLocalMessage(final ModelAction action,
                                   final int sendingNodeId,
                                   final int processingNodeId,
                                   final long modelZxid,
                                   List<Event> enabled) {
        for (final Event e : enabled) {
            if (e instanceof LocalEvent) {
                final LocalEvent event = (LocalEvent) e;
                final int eventNodeId = event.getNodeId();
                if (eventNodeId != processingNodeId) continue;
                final SubnodeType subnodeType = event.getSubnodeType();
                final int type = event.getType();
                switch (action) {
                    case LeaderSyncFollower:
                        LOG.debug("LeaderSyncFollower: {}, {}", subnodeType, type);
                        if (!subnodeType.equals(SubnodeType.LEARNER_HANDLER)
                                || type != TestingDef.MessageType.ACKEPOCH) continue;
                        final int followerNodeId = testingService.getFollowerSocketAddressBook().indexOf(event.getPayload());
                        if (sendingNodeId != followerNodeId) continue;
                        break;
                    case FollowerLogRequestWhenProcessingNEWLEADER:
                        LOG.debug("FollowerLogRequestWhenProcessingNEWLEADER: {}, {}", subnodeType, type);
                        if (!subnodeType.equals(SubnodeType.QUORUM_PEER)
                                || type != TestingDef.MessageType.NEWLEADER) continue;
                        break;
                    case LeaderLog:
                        final long eventZxid = event.getZxid();
                        if (!subnodeType.equals(SubnodeType.SYNC_PROCESSOR)) continue;
                        // since leaderLog always come first, here record the zxid mapping from model to code
                        // store model zxid for conformance checking later
                        if (modelZxid > 0) {
                            testingService.getModelToCodeZxidMap().put(modelZxid, eventZxid);
                            LOG.debug("LeaderLog, check getModelToCodeZxidMap: " +
                                            "modelZxid: {}-->CodeZxid: {}, eventZxid: {}",
                                    Long.toHexString(modelZxid),
                                    Long.toHexString(testingService.getModelToCodeZxidMap().get(modelZxid)),
                                    Long.toHexString(event.getZxid()));
                        } else {
                            LOG.debug("modelZxid {} < 0: no need to store model zxid for conformance checking",
                                    Long.toHexString(modelZxid));
                        }
                        break;
                    case FollowerLog:
                        if (!subnodeType.equals(SubnodeType.SYNC_PROCESSOR)) continue;
                        if (!checkZxidConformance(modelZxid, event.getZxid())) continue;
                        break;
                    case FollowerProcessPROPOSAL:  // DEPRECATED
                        if (!subnodeType.equals(SubnodeType.SYNC_PROCESSOR)) continue;
                        break;
                    case LeaderCommit:
                    case FollowerCommit:
                        if (!subnodeType.equals(SubnodeType.COMMIT_PROCESSOR)) continue;
                        if (!checkZxidConformance(modelZxid, event.getZxid())) continue;
                        break;
                    default:
                        continue;
                }
                nextEvent = event;
                events.remove(nextEvent);
                break;
            }
        }
    }

    private boolean checkZxidConformance(final long modelZxid, final long eventZxid) {
        // check the equality between zxid mapping from model to code
        // cases for conformance checking
        if (modelZxid > 0 && testingService.getModelToCodeZxidMap().containsKey(modelZxid)) {
            LOG.debug("-->> check getModelToCodeZxidMap: modelZxid: {} --> CodeZxid: {}. eventZxid: {}",
                    Long.toHexString(modelZxid),
                    Long.toHexString(testingService.getModelToCodeZxidMap().get(modelZxid)),
                    Long.toHexString(eventZxid));
            return eventZxid == testingService.getModelToCodeZxidMap().get(modelZxid);
        } else {
            LOG.debug("-->> modelZxid {} < 0 or no record: no need for conformance checking", Long.toHexString(modelZxid));
            return true;
        }
    }
}
