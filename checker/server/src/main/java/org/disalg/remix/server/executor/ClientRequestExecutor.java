package org.disalg.remix.server.executor;

import org.disalg.remix.api.SubnodeType;
import org.disalg.remix.api.state.LeaderElectionState;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClientRequestExecutor extends BaseEventExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRequestExecutor.class);

    private final ReplayService replayService;

    private int count = 5;

    private boolean waitForResponse = false;

    public ClientRequestExecutor(final ReplayService replayService) {
        this.replayService = replayService;
    }

    public ClientRequestExecutor(final ReplayService replayService, boolean waitForResponse, final int count) {
        this.replayService = replayService;
        this.waitForResponse = waitForResponse;
        this.count = count;
    }

    @Override
    public boolean execute(final ClientRequestEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed client request event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing client request event: {}", event.toString());
        releaseClientRequest(event);
        replayService.getControlMonitor().notifyAll();
        replayService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Client request executed: {}", event.toString());
        return true;
    }

    /***
     * The executor of client requests
     * @param event
     */
    public void releaseClientRequest(final ClientRequestEvent event) {
        final int clientId = event.getClientId();
        switch (event.getType()) {
            case GET_DATA:
                // TODO: this method should modify related states
//                for (int i = 0 ; i < schedulerConfiguration.getNumNodes(); i++) {
//                    nodeStateForClientRequests.set(i, NodeStateForClientRequest.SET_PROCESSING);
//                }
                replayService.getRequestQueue(clientId).offer(event);
                // Post-condition
                if (waitForResponse) {
                    // When we want to get the result immediately
                    // This will not generate later events automatically
                    replayService.getControlMonitor().notifyAll();
                    replayService.waitResponseForClientRequest(event);
                }
                // Note: the client request event may lead to deadlock easily
                //          when scheduled between some RequestProcessorEvents
//                final ClientRequestEvent clientRequestEvent =
//                        new ClientRequestEvent(replayService.generateEventId(), clientId,
//                                ClientRequestType.GET_DATA, this);
//                replayService.addEvent(clientRequestEvent);
                break;
            case SET_DATA:
            case CREATE:
//                for (int peer: replayService.getParticipants()) {
//                    replayService.getNodeStateForClientRequests().set(peer, NodeStateForClientRequest.SET_PROCESSING);
//                }

                replayService.getRequestQueue(clientId).offer(event);
                // Post-condition
//                replayService.waitResponseForClientRequest(event);
//                replayService.waitAllNodesSteadyAfterMutation();
                for (int node: replayService.getParticipants()) {
                    LeaderElectionState role = replayService.getLeaderElectionState(node);
                    switch (role) {
                        case LEADING:
                            replayService.getControlMonitor().notifyAll();
                            replayService.waitSubnodeTypeSending(node, SubnodeType.SYNC_PROCESSOR);
                            break;
                        case FOLLOWING:
                            replayService.getControlMonitor().notifyAll();
                            replayService.waitSubnodeInSendingState(replayService.getFollowerLearnerHandlerSenderMap(node));
                            break;
                        default:
                            break;
                    }
                }


                break;
        }
    }
}
