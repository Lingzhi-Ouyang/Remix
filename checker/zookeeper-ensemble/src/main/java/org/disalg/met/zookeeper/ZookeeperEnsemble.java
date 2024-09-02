package org.disalg.met.zookeeper;

import org.disalg.met.api.Ensemble;
import org.disalg.met.api.configuration.SchedulerConfigurationException;
import org.disalg.met.api.configuration.SchedulerConfigurationPostLoadListener;
import org.disalg.met.util.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ZookeeperEnsemble implements Ensemble, SchedulerConfigurationPostLoadListener {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperEnsemble.class);

    private static final String ZOOKEEPER_QUORUM_PEER_MAIN = "org.apache.zookeeper.server.quorum.QuorumPeerMain";

    @Autowired
    private ZookeeperConfiguration zookeeperConfiguration;

    private String executionId;

    private final List<Process> nodeProcesses = new ArrayList<>();


    @PostConstruct
    public void init() {
        zookeeperConfiguration.registerPostLoadListener(this);
    }

    @Override
    public void postLoadCallback() throws SchedulerConfigurationException {
        nodeProcesses.addAll(Collections.<Process>nCopies(zookeeperConfiguration.getNumNodes(), null));
    }

    @Override
    public void startNode(final int nodeId) {
        if (null != nodeProcesses.get(nodeId)) {
            LOG.warn("Node {} already started", nodeId);
            return;
        }

        LOG.debug("Starting node {}", nodeId);

        final File nodeDir = new File(zookeeperConfiguration.getWorkingDir(), executionId + File.separator + "nodes" + File.separator + nodeId);
        final File logDir = new File(nodeDir, "log");
        final File outputFile = new File(nodeDir, "out");
        final File confFile = new File(nodeDir, "conf");
        final File classpathFile = new File(zookeeperConfiguration.getWorkingDir(),
                ".." + File.separator + ".." + File.separator + zookeeperConfiguration.getClasspath());

        final String zookeeperLogDirOption = "-Dzookeeper.log.dir=" + logDir;
        final String appleAwtUIElementOption = "-Dapple.awt.UIElement=true"; // Suppress the Dock icon and menu bar on Mac OS X
        final String log4JConfigurationOption = "-Dlog4j.configuration=file:" + zookeeperConfiguration.getLog4JConfig();

        try {
            final Process process = ProcessUtil.startJavaProcess(zookeeperConfiguration.getWorkingDir(),
                    classpathFile.getAbsolutePath(), outputFile,
                    // Additional JVM options
                    zookeeperLogDirOption, appleAwtUIElementOption, log4JConfigurationOption,
                    // Class name and options
                    ZOOKEEPER_QUORUM_PEER_MAIN, confFile.getPath());
            nodeProcesses.set(nodeId, process);
            LOG.debug("Started node {}", nodeId);
        } catch (final IOException e) {
            LOG.error("Could not start node " + nodeId, e);
        }
    }

    @Override
    public void stopNode(final int nodeId) {
        if (null == nodeProcesses.get(nodeId)) {
            LOG.warn("Node {} is not running", nodeId);
            return;
        }

        LOG.debug("Stopping node {}", nodeId);
        final Process process = nodeProcesses.get(nodeId);
        process.destroy();
        try {
            process.waitFor();
            LOG.debug("Stopped node {}", nodeId);
        } catch (final InterruptedException e) {
            LOG.warn("Main thread interrupted while waiting for a process to terminate", e);
        } finally {
            nodeProcesses.set(nodeId, null);
        }
    }

    @Override
    public void startEnsemble() {
        LOG.debug("Starting the Zookeeper ensemble");
        for (int i = 0; i < zookeeperConfiguration.getNumNodes(); ++i) {
            startNode(i);
        }
        for (final Process process : nodeProcesses) {
            while (true) {
                if (null != process) {
                    LOG.debug("Process {} is not null", process);
                    break;
                }
            }
        }
    }

    @Override
    public void stopEnsemble() {
        LOG.debug("Stopping the Zookeeper ensemble");
        for (final Process process : nodeProcesses) {
            if (null != process) {
                process.destroy();
            }
        }
        for (int i = 0; i < zookeeperConfiguration.getNumNodes(); ++i) {
            final Process process = nodeProcesses.get(i);
            if (null != process) {
                try {
                    process.waitFor();
                    LOG.debug("Stopped node {}", i);
                } catch (final InterruptedException e) {
                    LOG.warn("Main thread interrupted while waiting for a process to terminate", e);
                } finally {
                    nodeProcesses.set(i, null);
                }
            }
        }
    }

    @Override
    public void configureEnsemble(final String executionId) throws SchedulerConfigurationException {
        this.executionId = executionId;
        for (int i = 0; i < zookeeperConfiguration.getNumNodes(); ++i) {
            zookeeperConfiguration.configureNode(executionId, i, "node");
        }
    }

    @Override
    public void configureEnsemble(final String executionId, final int serverNum) throws SchedulerConfigurationException {
        this.executionId = executionId;
        zookeeperConfiguration.setNumNodes(serverNum);
        for (int i = 0; i < serverNum; ++i) {
            zookeeperConfiguration.configureNode(executionId, i, "node");
        }
    }

}
