package org.disalg.remix.server.checker;

import org.disalg.remix.server.ReplayService;
import org.disalg.remix.server.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceVerifier implements Verifier{
    private static final Logger LOG = LoggerFactory.getLogger(TraceVerifier.class);

    private static int unmatchedCount = 0;
    private static int failedCount = 0;

    private final ReplayService replayService;
    private final Statistics statistics;
    private Integer traceLen;
    private Integer executedStep;

    // TODO: collect all verification statistics of a trace
    // all Match  & exits Failure


    public TraceVerifier(final ReplayService replayService, Statistics statistics) {
        this.replayService = replayService;
        this.statistics = statistics;
        this.traceLen = null;
        this.executedStep = null;
    }

    public void setTraceLen(Integer traceLen) {
        this.traceLen = traceLen;
    }

    public void setExecutedStep(Integer executedStep) {
        this.executedStep = executedStep;
    }

    public static int getUnmatchedCount() {
        return unmatchedCount;
    }

    public static int getFailedCount() {
        return failedCount;
    }

    @Override
    public boolean verify() {
        String passTest = replayService.tracePassed ? "PASS" : "FAILURE";

        String matchModel = "UNMATCHED";
        if (traceLen == null || executedStep == null) {
            matchModel = "UNKNOWN";
        } else if (executedStep >= traceLen) {
            matchModel = "MATCHED";
        }
        if (matchModel.equals("UNMATCHED")) {
            replayService.traceMatched = false;
        }
        statistics.reportResult("TRACE_EXECUTION:" + passTest + ":" + matchModel);

        if (!replayService.traceMatched) ++unmatchedCount;
        if (!replayService.tracePassed)  ++failedCount;

        return replayService.traceMatched && replayService.tracePassed;
    }
}
