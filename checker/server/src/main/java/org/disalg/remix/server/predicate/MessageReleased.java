package org.disalg.remix.server.predicate;

import org.disalg.remix.api.NodeState;
import org.disalg.remix.api.SubnodeState;
import org.disalg.remix.api.TestingDef;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the release of a message during election
 * When a node is stopping, this predicate will immediately be set true
 */
public class MessageReleased implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(MessageReleased.class);

    private final ReplayService replayService;

    private final int msgId;
    private final int sendingNodeId;
    private final Integer sendingSubnodeId;
    private final Event event;

    public MessageReleased(final ReplayService replayService, final int msgId, final int sendingNodeId) {
        this.replayService = replayService;
        this.msgId = msgId;
        this.sendingNodeId = sendingNodeId;
        this.sendingSubnodeId = null;
        this.event = null;
    }

    public MessageReleased(final ReplayService replayService,
                           final int msgId,
                           final int sendingNodeId,
                           final int sendingSubnodeId) {
        this.replayService = replayService;
        this.msgId = msgId;
        this.sendingNodeId = sendingNodeId;
        this.sendingSubnodeId = sendingSubnodeId;
        this.event = null;
    }

    public MessageReleased(final ReplayService replayService,
                           final int msgId,
                           final int sendingNodeId,
                           final int sendingSubnodeId,
                           final Event event) {
        this.replayService = replayService;
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
////                return NodeState.STOPPING.equals(replayService.getNodeStates().get(sendingNodeId)) ||
////                        event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
////            } else {
////                // message event
//                return replayService.getMessageInFlight() == msgId ||
//                        NodeState.STOPPING.equals(replayService.getNodeStates().get(sendingNodeId)) ||
//                        event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
//            }
            return replayService.getMessageInFlight() == msgId ||
                    NodeState.STOPPING.equals(replayService.getNodeStates().get(sendingNodeId)) ||
                    event.getFlag() == TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
        }
        if (sendingSubnodeId != null) {
            // other local event
            return replayService.getMessageInFlight() == msgId ||
                    NodeState.STOPPING.equals(replayService.getNodeStates().get(sendingNodeId)) ||
                    SubnodeState.UNREGISTERED.equals(replayService.getSubnodes().get(sendingSubnodeId).getState());
        } else {
            return replayService.getMessageInFlight() == msgId ||
                    NodeState.STOPPING.equals(replayService.getNodeStates().get(sendingNodeId));
        }
    }

    @Override
    public String describe() {
        return "release of message " + msgId + " sent by node " + sendingNodeId;
    }
}

