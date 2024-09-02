package org.disalg.met.server.predicate;

import org.disalg.met.api.ModelAction;
import org.disalg.met.api.configuration.SchedulerConfigurationException;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.Event;
import org.disalg.met.server.event.LocalEvent;
import org.disalg.met.server.scheduler.ExternalModelStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TargetInternalEventReady implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(TargetInternalEventReady.class);

    private final TestingService testingService;

    private final ExternalModelStrategy externalModelStrategy;

    private final ModelAction modelAction;

    private final Integer processingNodeId;

    private final Integer sendingNodeId;

    private Event event = null;

    private long modelZxid;

    public TargetInternalEventReady(final TestingService testingService,
                                    ExternalModelStrategy strategy,
                                    ModelAction action,
                                    Integer processingNodeId,
                                    Integer sendingNodeId,
                                    long modelZxid) {
        this.testingService = testingService;
        this.externalModelStrategy = strategy;
        this.modelAction = action;
        this.processingNodeId = processingNodeId;
        this.sendingNodeId = sendingNodeId;
        this.modelZxid = modelZxid;
    }

    public Event getEvent() {
        return event;
    }

    @Override
    public boolean isTrue() {
        try {
            event = externalModelStrategy.getNextInternalEvent(modelAction, processingNodeId, sendingNodeId, modelZxid);
        } catch (SchedulerConfigurationException e) {
            LOG.debug("SchedulerConfigurationException found when scheduling {}!", modelAction);
            return false;
        }
        return event != null;
    }

    @Override
    public String describe() {
        if (event instanceof LocalEvent) {
            return "target local event (action: " + modelAction +
                    " node: " + processingNodeId +
                    " ready";
        } else {
            return "target message event (action: " + modelAction +
                    " sending node: " + sendingNodeId +
                    ", receiving/processing node: " + processingNodeId +
                    " ready";
        }
    }
}
