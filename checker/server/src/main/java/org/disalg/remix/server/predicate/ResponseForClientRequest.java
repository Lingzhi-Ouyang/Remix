package org.disalg.remix.server.predicate;

import org.disalg.remix.api.state.ClientRequestType;
import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the result of a client request event
 */
public class ResponseForClientRequest implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseForClientRequest.class);

    private final ReplayService replayService;

    private final ClientRequestEvent event;

    public ResponseForClientRequest(final ReplayService replayService, final ClientRequestEvent event) {
        this.replayService = replayService;
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
                replayService.getReturnedDataList().add(result);
                LOG.debug("getReturnedData: {}", replayService.getReturnedDataList());
            }
        }
        return responseGot;
    }

    @Override
    public String describe() {
        return "response of " + event.toString();
    }
}
