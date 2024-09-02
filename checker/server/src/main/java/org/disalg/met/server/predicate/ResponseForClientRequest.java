package org.disalg.met.server.predicate;

import org.disalg.met.api.state.ClientRequestType;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the result of a client request event
 */
public class ResponseForClientRequest implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseForClientRequest.class);

    private final TestingService testingService;

    private final ClientRequestEvent event;

    public ResponseForClientRequest(final TestingService testingService, final ClientRequestEvent event) {
        this.testingService = testingService;
        this.event = event;
    }

    @Override
    //TODO: need to complete
    public boolean isTrue() {
        boolean responseGot = false;
        String result = event.getResult();
        if(result != null){
            responseGot = true;
            if (event.getType().equals(ClientRequestType.GET_DATA) ) {
                testingService.getReturnedDataList().add(result);
                LOG.debug("getReturnedData: {}", testingService.getReturnedDataList());
            }
        }
        return responseGot;
    }

    @Override
    public String describe() {
        return "response of " + event.toString();
    }
}
