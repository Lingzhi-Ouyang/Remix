package org.disalg.met.server;

import org.junit.Test;
import org.disalg.met.server.event.DummyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EventTest {

    private static final Logger LOG = LoggerFactory.getLogger(EventTest.class);

    @Test
    public void testHappensBefore() {
        final DummyEvent a = new DummyEvent();
        final DummyEvent b = new DummyEvent();
        final DummyEvent c = new DummyEvent();
        final DummyEvent d = new DummyEvent();

        d.addAllDirectPredecessors(Arrays.asList(b, c));
        b.addDirectPredecessor(a);

        assertTrue("a should happen before b", a.happensBefore(b));
        assertTrue("a should happen before d", a.happensBefore(d));
        assertFalse("a should not happen before c", a.happensBefore(c));
    }
}
