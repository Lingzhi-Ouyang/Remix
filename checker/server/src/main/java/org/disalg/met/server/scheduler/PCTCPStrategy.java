package org.disalg.met.server.scheduler;

import org.disalg.met.server.statistics.PCTCPStatistics;
import org.disalg.met.server.event.Event;
import org.disalg.met.server.event.DummyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PCTCPStrategy implements SchedulingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PCTCPStrategy.class);

    private final PCTCPRandom random;
    private final Map<Integer, Integer> priorityChangePoints;
    private final List<Chain> chains;
    private final List<List<Chain>> chainPartition = new ArrayList<>();

    private final DummyEvent dummyEvent = new DummyEvent();
    private final int maxEvents;
    private int eventsAdded = 0;
    private int eventsPrepared = 0;

    private final PCTCPStatistics statistics;

    public PCTCPStrategy(int maxEvents, int numPriorityChangePoints, final Random random, final PCTCPStatistics statistics) {
        this.maxEvents = maxEvents;
        this.random = new PCTCPRandom(maxEvents, numPriorityChangePoints, random);
        this.chains = new ArrayList<>(Collections.<Chain>nCopies(numPriorityChangePoints, null));
        this.priorityChangePoints = this.random.generatePriorityChangePoints();
        this.statistics = statistics;
        this.statistics.reportPriorityChangePoints(priorityChangePoints);
    }

    private boolean nextEventPrepared = false;
    private Event nextEvent;

    @Override
    public void remove(Event event) {
        LOG.warn("Not implementation of removing event: {}", event.toString());
    }

    @Override
    public boolean hasNextEvent() {
        if (!nextEventPrepared) {
            prepareNextEvent();
        }
        LOG.debug("hasNextEvent == {}", nextEvent != null);
        return nextEvent != null;
    }

    @Override
    public Event nextEvent() {
        if (!nextEventPrepared) {
            prepareNextEvent();
        }
        nextEventPrepared = false;
        return nextEvent;
    }

    private void prepareNextEvent() {
        LOG.debug("Preparing next event...");
        if (eventsPrepared == maxEvents) {
            dummyEvent.execute();
        }
        nextEvent = null;
        int totalEnabled = 0;
        for (int i = chains.size() - 1; i >= 0; i--) {
            final Chain chain = chains.get(i);
            if (chain == null || chain.isEmpty()) {
                continue;
            }
            final Event event = chain.peekFirst();
            if (event.hasLabel() && event.getLabel() != i) {
                Collections.swap(chains, i, event.getLabel());
                /***
                 * Note that since labels are unique, this must be the first occurrence of the particular label,
                 * so chains.get(event.getLabel()) must be null before the swap, that is, chains.get(i) must be null
                 * after the swap. Thus, if event.getLabel() < i, we can simply continue -- we act as if the current
                 * chain does not exist, and we will encounter will encounter the same event later. But if
                 * event.getLabel() > i, we must proceed to check whether the current event is enabled -- if it is,
                 * we must prepare it since it has the highest priority.
                 */
                if (event.getLabel() < i) {
                    continue;
                }
            }
            if (event.isEnabled()) {
                totalEnabled++;
                if (!nextEventPrepared) {
                    nextEvent = event;
                    chain.removeFirst();
                    eventsPrepared++;
                    nextEventPrepared = true;
                }
            }
        }
        nextEventPrepared = true;
        statistics.reportNumberOfChains(chains.size());
        statistics.reportNumberOfEnabledEvents(totalEnabled);
    }

    @Override
    public void add(final Event event) {
        eventsAdded++;
        if (priorityChangePoints.containsKey(eventsAdded)) {
            event.setLabel(priorityChangePoints.get(eventsAdded));
        }
        if (eventsAdded > maxEvents) {
            // Make sure the event stays disabled until the first maxEvents events are dispatched
            event.addDirectPredecessor(dummyEvent);
        }
        LOG.debug("Adding event: {}", event.toString());

        for (int i = 0; i < chainPartition.size(); i++) {
            final List<Chain> block = chainPartition.get(i);

            final Iterator<Chain> blockIterator = block.iterator();
            while (blockIterator.hasNext()) {
                final Chain chain = blockIterator.next();
                if (chain.peekLast().happensBefore(event)) {
                    chain.addLast(event);
                    if (i > 0) {
                        blockIterator.remove();
                        chainPartition.get(i - 1).add(chain);
                        Collections.swap(chainPartition, i - 1, i);
                    }
                    return;
                }
            }

            if (block.size() <= i) {
                block.add(createChain(event));
                return;
            }
        }

        final List<Chain> block = new LinkedList<>();
        block.add(createChain(event));
        chainPartition.add(block);

        // Make sure prepareNextEvent() is triggered
        if (nextEventPrepared && nextEvent == null) {
            nextEventPrepared = false;
        }
    }

    private Chain createChain(final Event event) {
        final Chain chain = new Chain(event);
        final int chainPosition = random.generateChainPosition(chains.size());
        chains.add(chainPosition, chain);
        return chain;
    }

    private final class Chain {

        private final Deque<Event> deque;

        private Event lastTail;

        public Chain(final Event event) {
            deque = new ArrayDeque<>();
            addLast(event);
        }

        public boolean isEmpty() {
            return deque.isEmpty();
        }

        public Event peekFirst() {
            return deque.peekFirst();
        }

        public Event peekLast() {
            return lastTail;
        }

        public Event removeFirst() {
            return deque.removeFirst();
        }

        public void addLast(final Event event) {
            deque.addLast(event);
            lastTail = event;
        }
    }
}
