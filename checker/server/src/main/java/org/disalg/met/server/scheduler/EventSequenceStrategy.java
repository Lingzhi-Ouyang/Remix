package org.disalg.met.server.scheduler;

import org.disalg.met.server.event.LeaderToFollowerMessageEvent;
import org.disalg.met.server.event.LocalEvent;
import org.disalg.met.api.MessageType;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.configuration.SchedulerConfigurationException;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.Event;
import org.disalg.met.server.state.Subnode;
import org.disalg.met.server.statistics.ExternalModelStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class EventSequenceStrategy implements SchedulingStrategy{

    private static final Logger LOG = LoggerFactory.getLogger(EventSequenceStrategy.class);

    private final TestingService testingService;

    private final Random random;

    private File dir;
    private File[] files;
    private List<EventSequence> traces = new LinkedList<>();
    private int count = 0;
    private EventSequence currentTrace = null;

    private boolean nextEventPrepared = false;
    private Event nextEvent = null;
    private final Set<Event> events = new HashSet<>();

    private final ExternalModelStatistics statistics;

    public EventSequenceStrategy(TestingService testingService, Random random, File dir, final ExternalModelStatistics statistics) throws SchedulerConfigurationException {
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

    public EventSequence getCurrentTrace(final int idx) {
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
                    EventSequence trace = importTrace(file);
                    if (null == trace) continue;
                    traces.add(trace);
                    count++;
                    LOG.debug("trace: {}", trace.toString());
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

    public EventSequence importTrace(File file) throws IOException {
        String filename = file.getName();
        if(filename.startsWith(".")) {
            return null;
        }
        LOG.debug("Importing trace from file {}", filename);
        InputStreamReader read = null;
        try {
            read = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert read != null;
        BufferedReader bufferedReader = new BufferedReader(read);
        EventSequence trace = new EventSequence(filename);
        String lineTxt;
        while ((lineTxt = bufferedReader.readLine()) != null) {
            trace.addStep(lineTxt);
            String[] lineArr = lineTxt.split(" ");
            int len = lineArr.length;
            LOG.debug(lineTxt);
        }
        read.close();
        return trace;
    }

    public Event getNextInternalEvent(String[] lineArr) throws SchedulerConfigurationException {
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

        nextEvent = null;
        assert enabled.size() > 0;

        // 2. search specific event type
        int len = lineArr.length;
        String action = lineArr[0];

        switch (action) {
            case "LOG_REQUEST":
            case "COMMIT":
                searchRequestEvent(action, len, lineArr, enabled);
                break;
            case "SEND_PROPOSAL":
            case "SEND_COMMIT":
                searchLearnerHandlerMessage(action, len, lineArr, enabled);
                break;
        }

        if ( nextEvent != null){
            LOG.debug("next event exists! {}", nextEvent);
        } else {
            throw new SchedulerConfigurationException();
        }

        nextEventPrepared = true;
        return nextEvent;
    }

    private void searchRequestEvent(String action, int len, String[] lineArr, List<Event> enabled) {
        for (final Event e : enabled) {
            if (e instanceof LocalEvent) {
                assert len >= 2;

                final LocalEvent event = (LocalEvent) e;
                final Long zxid = len > 2 ? Long.parseLong(lineArr[2]) : null;
                if (zxid != null) {
                    final long eventZxid = event.getZxid();
                    if (!zxid.equals(eventZxid)) continue;
                }

                final int serverId = Integer.parseInt(lineArr[1]);
                final int nodeId = event.getNodeId();
                if (serverId != nodeId) continue;

                final SubnodeType subnodeType = event.getSubnodeType();
                switch (action) {
                    case "LOG_REQUEST":
                        if (!SubnodeType.SYNC_PROCESSOR.equals(subnodeType)) continue;
                        break;
                    case "COMMIT":
                        if (!SubnodeType.COMMIT_PROCESSOR.equals(subnodeType)) continue;
                        break;
                }
                nextEvent = event;
                events.remove(nextEvent);
                break;
            }
        }
    }

    public void searchLearnerHandlerMessage(String action, int len, String[] lineArr, List<Event> enabled) {
        for (final Event e : enabled) {
            if (e instanceof LeaderToFollowerMessageEvent) {
                assert len >= 3;

                final LeaderToFollowerMessageEvent event = (LeaderToFollowerMessageEvent) e;
                final Long zxid = len > 3 ? Long.parseLong(lineArr[3]) : null;
                if (zxid != null) {
                    final long eventZxid = event.getZxid();
                    if (!zxid.equals(eventZxid)) continue;
                }

                final int s1 = Integer.parseInt(lineArr[1]);
                final int s2 = Integer.parseInt(lineArr[2]);
                final int receivingNodeId = event.getReceivingNodeId();
                final int sendingSubnodeId = event.getSendingSubnodeId();
                final Subnode sendingSubnode = testingService.getSubnodes().get(sendingSubnodeId);
                final int sendingNodeId = sendingSubnode.getNodeId();
                if (sendingNodeId != s1 || receivingNodeId != s2) continue;

                final int type = event.getType();
                switch (action) {
                    case "SEND_PROPOSAL":
                        if (type != MessageType.PROPOSAL) continue;
                        break;
                    case "SEND_COMMIT":
                        if (type != MessageType.COMMIT) continue;
                        break;
                }
                nextEvent = event;
                events.remove(nextEvent);
                break;
            }
        }
    }

}
