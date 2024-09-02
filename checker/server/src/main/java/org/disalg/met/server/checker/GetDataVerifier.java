package org.disalg.met.server.checker;

import org.disalg.met.server.TestingService;
import org.disalg.met.server.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GetDataVerifier implements Verifier{

    private static final Logger LOG = LoggerFactory.getLogger(GetDataVerifier.class);

    private final TestingService testingService;
    private final Statistics statistics;
    private String modelResult;

    public GetDataVerifier(final TestingService testingService, Statistics statistics) {
        this.testingService = testingService;
        this.statistics = statistics;
        this.modelResult = null;
    }

    public void setModelResult(String modelResult) {
        this.modelResult = modelResult;
    }

    @Override
    public boolean verify() {
        String matchModel = "UNMATCHED";
        List<String> returnedDataList = testingService.getReturnedDataList();
        LOG.debug("verify getReturnedData: {}, model: {}", testingService.getReturnedDataList(), modelResult);
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
            testingService.traceMatched = false;
        }
        if (result) {
            statistics.reportResult("MONOTONIC_READ:SUCCESS:" + matchModel);
            return true;
        }
        else {
            statistics.reportResult("MONOTONIC_READ:FAILURE:" + matchModel);
            testingService.tracePassed = false;
            return false;
        }
    }
}
