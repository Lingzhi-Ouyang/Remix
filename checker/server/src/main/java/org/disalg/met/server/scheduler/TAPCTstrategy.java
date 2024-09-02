package org.disalg.met.server.scheduler;

import org.disalg.met.server.statistics.TAPCTstatistics;
import org.disalg.met.server.event.ElectionMessageEvent;
import org.disalg.met.server.event.Event;
import org.disalg.met.server.event.DummyEvent;
import org.disalg.met.server.event.NodeCrashEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TAPCTstrategy implements SchedulingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(TAPCTstrategy.class);

    private final TAPCTrandom random;
    private final Map<Integer, Integer> priorityChangePoints;
    private final List<Chain> chains;
    private final List<List<Chain>> chainPartition = new ArrayList<>();

    private final DummyEvent dummyEvent = new DummyEvent();
    private final int maxEvents;
    private int eventsAdded = 0;
    private int eventsPrepared = 0;

    private final TAPCTstatistics statistics;


    private int numPriorityChangePoints;
    private int numRacy = 0;

    public TAPCTstrategy(int maxEvents, int numPriorityChangePoints, final Random random, final TAPCTstatistics statistics) {
        this.maxEvents = maxEvents;
        this.random = new TAPCTrandom(maxEvents, numPriorityChangePoints, random);
        this.chains = new ArrayList<>(Collections.<Chain>nCopies(numPriorityChangePoints, null));
        this.priorityChangePoints = this.random.generatePriorityChangePoints();
        this.statistics = statistics;
        this.statistics.reportPriorityChangePoints(priorityChangePoints);
        this.numPriorityChangePoints = numPriorityChangePoints;
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
        LOG.debug("chains.size: {}", chains.size());
        for (int i = chains.size() - 1; i >= 0; i--) {
            boolean reachedPCP = false;
            final Chain chain = chains.get(i);
            if (chain == null || chain.isEmpty()) {
                continue;
            }
            final Event event = chain.peekFirst();
            if (event.isEnabled()) {
                totalEnabled++;
                for (int j = i - 1; j >= 0; j--){
                    if (i >= numPriorityChangePoints && j < numPriorityChangePoints) {
                        break;
                    }
                    Chain chainToCompare = chains.get(j);
                    if (chainToCompare == null || chainToCompare.isEmpty()) {
                        continue;
                    }
                    Event eventToCompare = chainToCompare.peekFirst();
                    if (eventToCompare.isEnabled() && isRacy(event, eventToCompare)){
                        numRacy++;
                        statistics.reportNumRacy(numRacy);
                        if (priorityChangePoints.containsKey(numRacy)){
                            Collections.swap(chains, i, priorityChangePoints.get(numRacy));
                            reachedPCP = true;
                            i = chains.size();
                            break;
                        }
                    }

                }
                if (!reachedPCP && !nextEventPrepared) {
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
        if (eventsAdded > maxEvents) {
            // Make sure the event stays disabled until the first maxEvents events are dispatched
            event.addDirectPredecessor(dummyEvent);
        }
        LOG.debug("Adding event: {}", event.toString());
        LOG.debug("chainPartition: {}, size: {}", chainPartition, chainPartition.size());
        for (int i = 0; i < chainPartition.size(); i++) {
            final List<Chain> block = chainPartition.get(i);
            LOG.debug("chainPartition({}): {}", i, block);

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
        LOG.debug("chains.size(): {}", chains.size());
        final int chainPosition = random.generateChainPosition(chains.size());
        LOG.debug("chains.add({}, {})", chainPosition, chain);
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
    private boolean isRacy (Event e1, Event e2){
        if (e1 instanceof ElectionMessageEvent && e2 instanceof ElectionMessageEvent) {
            if (((ElectionMessageEvent) e1).getReceivingNodeId() == ((ElectionMessageEvent) e2).getReceivingNodeId()) {
                return true;
            }
        }
        if (e1 instanceof ElectionMessageEvent && e2 instanceof NodeCrashEvent) {
            if (((ElectionMessageEvent) e1).getReceivingNodeId() == ((NodeCrashEvent) e2).getNodeId()) {
                return true;
            }
        }
        return false;
    }
}
