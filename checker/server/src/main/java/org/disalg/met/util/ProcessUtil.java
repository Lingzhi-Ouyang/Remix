package org.disalg.met.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProcessUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessUtil.class);

    public static String getJava() {
        final String javaHome = System.getProperty("java.home");
        return javaHome + File.separator + "bin" + File.separator + "java";
    }

    public static String getClasspath() {
        return System.getProperty("java.class.path");
    }

    public static Process startJavaProcess(final File workingDir, final String classpath, final File output, final String... cmd) throws IOException {
        final List<String> finalCmd = new ArrayList<>();
        finalCmd.add(getJava());
        if (null != classpath) {
            finalCmd.addAll(Arrays.asList("-classpath", classpath));
        }
        finalCmd.addAll(Arrays.asList(cmd));

        LOG.debug("Starting a process with the following command: {}", finalCmd.toString());

        final ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(finalCmd);
        processBuilder.directory(workingDir);
        processBuilder.redirectOutput(output);
        processBuilder.redirectErrorStream(true);
        return processBuilder.start();
    }

}
