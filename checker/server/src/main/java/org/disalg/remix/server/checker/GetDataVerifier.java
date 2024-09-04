package org.disalg.remix.server.checker;

import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GetDataVerifier implements Verifier{

    private static final Logger LOG = LoggerFactory.getLogger(GetDataVerifier.class);

    private final ReplayService replayService;
    private final Statistics statistics;
    private String modelResult;

    public GetDataVerifier(final ReplayService replayService, Statistics statistics) {
        this.replayService = replayService;
        this.statistics = statistics;
        this.modelResult = null;
    }

    public void setModelResult(String modelResult) {
        this.modelResult = modelResult;
    }

    @Override
    public boolean verify() {
        String matchModel = "UNMATCHED";
        List<String> returnedDataList = replayService.getReturnedDataList();
        LOG.debug("verify getReturnedData: {}, model: {}", replayService.getReturnedDataList(), modelResult);
        final int len = returnedDataList.size();
        assert len >= 2;
        final String latestOne = returnedDataList.get(len - 1);
        final String latestSecond = returnedDataList.get(len - 2);
        boolean result = Long.parseLong(latestOne, 16) >= Long.parseLong(latestSecond, 16);
//        boolean result = (latestOne.length() > latestSecond.length()) ||
//                (latestOne.length() == latestSecond.length() && latestOne.compareTo(latestSecond) >= 0);
        if (this.modelResult == null) {
            matchModel = "UNKNOWN";
        } else if (this.modelResult.equals(latestOne)){
            matchModel = "MATCHED";
        }
        if (matchModel.equals("UNMATCHED")) {
            replayService.traceMatched = false;
        }
        if (result) {
            statistics.reportResult("MONOTONIC_READ:SUCCESS:" + matchModel);
            return true;
        }
        else {
            statistics.reportResult("MONOTONIC_READ:FAILURE:" + matchModel);
            replayService.tracePassed = false;
            return false;
        }
    }
}
