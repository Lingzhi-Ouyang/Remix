package org.disalg.met.zookeeper;

import org.disalg.met.api.ClientGroup;
import org.disalg.met.api.configuration.SchedulerConfigurationException;
import org.disalg.met.api.configuration.SchedulerConfigurationPostLoadListener;
import org.disalg.met.util.ProcessUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ZooKeeperClientGroup implements ClientGroup, SchedulerConfigurationPostLoadListener {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClientGroup.class);

    private static final String ZOOKEEPER_CLIENT_MAIN = "org.apache.zookeeper.ZooKeeperMain";

    @Autowired
    private ZookeeperConfiguration zookeeperConfiguration;

    private int executionId;

    private final List<Process> clientProcesses = new ArrayList<>();

    @PostConstruct
    public void init() {
        zookeeperConfiguration.registerPostLoadListener(this);
    }

    @Override
    public void postLoadCallback() throws SchedulerConfigurationException {
        clientProcesses.addAll(Collections.<Process>nCopies(zookeeperConfiguration.getNumClients(), null));
    }

    @Override
    public void startClient(int clientId) {
        if (null != clientProcesses.get(clientId)) {
            LOG.warn("ClientGroup {} already started", clientId);
            return;
        }

        LOG.debug("Starting client {}", clientId);

        final File clientDir = new File(zookeeperConfiguration.getWorkingDir(), executionId + File.separator + "clients" + File.separator + clientId);
        final File logDir = new File(clientDir, "log");
        final File outputFile = new File(clientDir, "out");
        final File confFile = new File(clientDir, "conf");
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
                    ZOOKEEPER_CLIENT_MAIN, "-server", "127.0.0.1:4000", "ls", "/");

            clientProcesses.set(clientId, process);
            LOG.debug("Started client {}", clientId);
        } catch (final IOException e) {
            LOG.error("Could not start client " + clientId, e);
        }
    }

    @Override
    public void stopClient(int client) {

    }

    @Override
    public void configureClients(final int executionId) throws SchedulerConfigurationException {
        this.executionId = executionId;
        for (int i = 0; i < zookeeperConfiguration.getNumClients(); ++i) {
            zookeeperConfiguration.configureNode(String.valueOf(executionId), i, "client");
        }
    }

    @Override
    public void startClients() {
        LOG.debug("Starting the Zookeeper clients");
        for (int i = 0; i < zookeeperConfiguration.getNumClients(); ++i) {
            startClient(i);
        }
        for (final Process process : clientProcesses) {
            while (true) {
                if (null != process) {
                    LOG.debug("ClientGroup process {} is not null", process);
                    break;
                }
            }
        }
    }

    @Override
    public void stopClients() {
        LOG.debug("Stopping the Zookeeper clients");
        for (final Process process : clientProcesses) {
            if (null != process) {
                process.destroy();
            }
        }
        for (int i = 0; i < zookeeperConfiguration.getNumClients(); ++i) {
            final Process process = clientProcesses.get(i);
            if (null != process) {
                try {
                    process.waitFor();
                    LOG.debug("Stopped client {}", i);
                } catch (final InterruptedException e) {
                    LOG.warn("Main thread interrupted while waiting for a process to terminate", e);
                } finally {
                    clientProcesses.set(i, null);
                }
            }
        }
    }
}
