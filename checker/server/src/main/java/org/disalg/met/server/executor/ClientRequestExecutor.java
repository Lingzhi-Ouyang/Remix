package org.disalg.met.server.executor;

import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.state.LeaderElectionState;
import org.disalg.met.server.TestingService;
import org.disalg.met.api.NodeStateForClientRequest;
import org.disalg.met.api.state.ClientRequestType;
import org.disalg.met.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClientRequestExecutor extends BaseEventExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRequestExecutor.class);

    private final TestingService testingService;

    private int count = 5;

    private boolean waitForResponse = false;

    public ClientRequestExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    public ClientRequestExecutor(final TestingService testingService, boolean waitForResponse, final int count) {
        this.testingService = testingService;
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
        testingService.getControlMonitor().notifyAll();
        testingService.waitAllNodesSteady();
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
                testingService.getRequestQueue(clientId).offer(event);
                // Post-condition
                if (waitForResponse) {
                    // When we want to get the result immediately
                    // This will not generate later events automatically
                    testingService.getControlMonitor().notifyAll();
                    testingService.waitResponseForClientRequest(event);
                }
                // Note: the client request event may lead to deadlock easily
                //          when scheduled between some RequestProcessorEvents
//                final ClientRequestEvent clientRequestEvent =
//                        new ClientRequestEvent(testingService.generateEventId(), clientId,
//                                ClientRequestType.GET_DATA, this);
//                testingService.addEvent(clientRequestEvent);
                break;
            case SET_DATA:
            case CREATE:
//                for (int peer: testingService.getParticipants()) {
//                    testingService.getNodeStateForClientRequests().set(peer, NodeStateForClientRequest.SET_PROCESSING);
//                }

                testingService.getRequestQueue(clientId).offer(event);
                // Post-condition
//                testingService.waitResponseForClientRequest(event);
//                testingService.waitAllNodesSteadyAfterMutation();
                for (int node: testingService.getParticipants()) {
                    LeaderElectionState role = testingService.getLeaderElectionState(node);
                    switch (role) {
                        case LEADING:
                            testingService.getControlMonitor().notifyAll();
                            testingService.waitSubnodeTypeSending(node, SubnodeType.SYNC_PROCESSOR);
                            break;
                        case FOLLOWING:
                            testingService.getControlMonitor().notifyAll();
                            testingService.waitSubnodeInSendingState(testingService.getFollowerLearnerHandlerSenderMap(node));
                            break;
                        default:
                            break;
                    }
                }


                break;
        }
    }
}
