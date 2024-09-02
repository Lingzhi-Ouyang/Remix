package org.disalg.met.server;

import org.junit.Test;
import org.disalg.met.server.event.Event;
import org.disalg.met.server.scheduler.PCTCPStrategy;
import org.disalg.met.server.scheduler.SchedulingStrategy;
import org.disalg.met.server.statistics.PCTCPStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class PCTCPStrategyTest {

    private static final Logger LOG = LoggerFactory.getLogger(PCTCPStrategyTest.class);

    @Test
    public void testSchedulingStrategy() {
        final StringBuilder stringBuilder = new StringBuilder();
        final NamedEventExecutor executor = new NamedEventExecutor(stringBuilder);
        final Map<String, Integer> histogram = new HashMap<>();

        for (int i = 0; i < 1000; i++) {
            final NamedEvent a = new NamedEvent("a", executor);
            final NamedEvent b = new NamedEvent("b", executor);
            final NamedEvent c = new NamedEvent("c", executor);
            final NamedEvent d = new NamedEvent("d", executor);

            b.addDirectPredecessor(a);
            d.addAllDirectPredecessors(Arrays.asList(b, c));

            final PCTCPStatistics statistics = new PCTCPStatistics();
            final SchedulingStrategy schedulingStrategy = new PCTCPStrategy(4, 1, new Random(), statistics);
            for (final Event event : Arrays.asList(a, b, c, d)) {
                schedulingStrategy.add(event);
            }

            stringBuilder.setLength(0);
            while (schedulingStrategy.hasNextEvent()) {
                final Event event = schedulingStrategy.nextEvent();
                try {
                    event.execute();
                } catch (final IOException e) {
                    LOG.debug("IO exception", e);
                }
            }
            final String execution = stringBuilder.toString();
            if (!histogram.containsKey(execution)) {
                histogram.put(execution, 0);
            }
            final int count = histogram.get(execution);
            histogram.put(execution, count + 1);
        }

        for (final Map.Entry<String, Integer> entry : histogram.entrySet()) {
            LOG.debug("Execution {}: {}", entry.getKey(), entry.getValue());
        }
    }
}
