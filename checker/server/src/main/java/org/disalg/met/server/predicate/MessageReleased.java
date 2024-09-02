package org.disalg.met.server.predicate;

import org.disalg.met.api.NodeState;
import org.disalg.met.api.SubnodeState;
import org.disalg.met.api.TestingDef;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.Event;
import org.disalg.met.server.event.LocalEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the release of a message during election
 * When a node is stopping, this predicate will immediately be set true
 */
public class MessageReleased implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(MessageReleased.class);

    private final TestingService testingService;

    private final int msgId;
    private final int sendingNodeId;
    private final Integer sendingSubnodeId;
    private final Event event;

    public MessageReleased(final TestingService testingService, final int msgId, final int sendingNodeId) {
        this.testingService = testingService;
        this.msgId = msgId;
        this.sendingNodeId = sendingNodeId;
        this.sendingSubnodeId = null;
        this.event = null;
    }

    public MessageReleased(final TestingService testingService,
                           final int msgId,
                           final int sendingNodeId,
                           final int sendingSubnodeId) {
        this.testingService = testingService;
        this.msgId = msgId;
        this.sendingNodeId = sendingNodeId;
        this.sendingSubnodeId = sendingSubnodeId;
        this.event = null;
    }

    public MessageReleased(final TestingService testingService,
                           final int msgId,
                           final int sendingNodeId,
                           final int sendingSubnodeId,
                           final Event event) {
        this.testingService = testingService;
        this.msgId = msgId;
        this.sendingNodeId = sendingNodeId;
        this.sendingSubnodeId = sendingSubnodeId;
        this.event = event;
    }



    @Override
    public boolean isTrue() {
        if (event != null) {
//            if (event instanceof LocalEvent) {
//////                 LeaderJudgingIsRunning
////                return NodeState.STOPPING.equals(testingService.getNodeStates().get(sendingNodeId)) ||
////                        event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
////            } else {
////                // message event
//                return testingService.getMessageInFlight() == msgId ||
//                        NodeState.STOPPING.equals(testingService.getNodeStates().get(sendingNodeId)) ||
//                        event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
//            }
            return testingService.getMessageInFlight() == msgId ||
                    NodeState.STOPPING.equals(testingService.getNodeStates().get(sendingNodeId)) ||
                    event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
        }
        if (sendingSubnodeId != null) {
            // other local event
            return testingService.getMessageInFlight() == msgId ||
                    NodeState.STOPPING.equals(testingService.getNodeStates().get(sendingNodeId)) ||
                    SubnodeState.UNREGISTERED.equals(testingService.getSubnodes().get(sendingSubnodeId).getState());
        } else {
            return testingService.getMessageInFlight() == msgId ||
                    NodeState.STOPPING.equals(testingService.getNodeStates().get(sendingNodeId));
        }
    }

    @Override
    public String describe() {
        return "release of message " + msgId + " sent by node " + sendingNodeId;
    }
}

