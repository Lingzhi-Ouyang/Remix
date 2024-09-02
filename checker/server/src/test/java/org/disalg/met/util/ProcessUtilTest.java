package org.disalg.met.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessUtilTest {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessUtilTest.class);

    @Test
    public void testGetJava() {
        final String java = ProcessUtil.getJava();
        LOG.debug("java = {}", java);
    }

    @Test
    public void testGetClasspath() {
        final String classpath = ProcessUtil.getClasspath();
        LOG.debug("classpath = {}", classpath);
    }

}
