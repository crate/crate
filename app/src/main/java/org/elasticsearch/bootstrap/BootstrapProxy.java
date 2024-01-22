/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.bootstrap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.IfConfig;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.discovery.ec2.Ec2DiscoveryPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugin.repository.url.URLRepositoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.transport.Netty4Plugin;

import io.crate.bootstrap.BootstrapException;
import io.crate.common.SuppressForbidden;
import io.crate.plugin.SrvPlugin;
import io.crate.udc.plugin.UDCPlugin;

/**
 * <p>
 * This is a copy of ES src/main/java/org/elasticsearch/bootstrap/Bootstrap.java.
 * <p>
 * With following patches:
 * - CrateNode instead of Node is build/started in order to load the CrateCorePlugin
 * - CrateSettingsPreparer is used instead of InternalSettingsPreparer
 * - disabled security manager setup due to policy problems with plugins
 */
public class BootstrapProxy {

    private static volatile BootstrapProxy INSTANCE;

    private static final Collection<Class<? extends Plugin>> DEFAULT_PLUGINS = List.of(
        SrvPlugin.class,
        UDCPlugin.class,
        URLRepositoryPlugin.class,
        S3RepositoryPlugin.class,
        Ec2DiscoveryPlugin.class,
        Netty4Plugin.class);

    private volatile Node node;
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    private final Thread keepAliveThread;

    /**
     * creates a new instance
     */
    BootstrapProxy() {
        keepAliveThread = new Thread(() -> {
            try {
                keepAliveLatch.await();
            } catch (InterruptedException e) {
                // bail out
            }
        }, "crate[keepAlive/" + Version.CURRENT + "]");
        keepAliveThread.setDaemon(false);
        // keep this thread alive (non daemon thread) until we shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(keepAliveLatch::countDown));
    }

    /**
     * initialize native resources
     */
    static void initializeNatives(boolean mlockAll, boolean ctrlHandler) {
        final Logger logger = LogManager.getLogger(BootstrapProxy.class);

        // check if the user is running as root, and bail
        if (Natives.definitelyRunningAsRoot()) {
            throw new RuntimeException("can not run crate as root");
        }

        // mlockall if requested
        if (mlockAll) {
            if (Constants.WINDOWS) {
                Natives.tryVirtualLock();
            } else {
                Natives.tryMlockall();
            }
        }

        // listener for windows close event
        if (ctrlHandler) {
            Natives.addConsoleCtrlHandler(code -> {
                if (ConsoleCtrlHandler.CTRL_CLOSE_EVENT == code) {
                    logger.info("running graceful exit on windows");
                    try {
                        BootstrapProxy.stop();
                    } catch (IOException e) {
                        throw new ElasticsearchException("failed to stop node", e);
                    }
                    return true;
                }
                return false;
            });
        }

        // force remainder of JNA to be loaded (if available).
        try {
            JNAKernel32Library.getInstance();
        } catch (Exception ignored) {
            // we've already logged this.
        }

        Natives.trySetMaxNumberOfThreads();
        Natives.trySetMaxSizeVirtualMemory();
        Natives.trySetMaxFileSize();

        // init lucene random seed. it will use /dev/urandom where available:
        StringHelper.randomId();
    }

    static void initializeProbes() {
        // Force probes to be loaded
        ProcessProbe.getInstance();
        OsProbe.getInstance();
        JvmInfo.jvmInfo();
    }

    private void setup(boolean addShutdownHook, Environment environment) throws BootstrapException {
        Settings settings = environment.settings();
        initializeNatives(
            BootstrapSettings.MEMORY_LOCK_SETTING.get(settings),
            BootstrapSettings.CTRLHANDLER_SETTING.get(settings));

        // initialize probes before the security manager is installed
        initializeProbes();

        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    IOUtils.close(node);
                    LoggerContext context = (LoggerContext) LogManager.getContext(false);
                    Configurator.shutdown(context);
                    if (node != null && node.awaitClose(10, TimeUnit.SECONDS) == false) {
                        throw new IllegalStateException(
                            "Node didn't stop within 10 seconds. " +
                            "Any outstanding requests or tasks might get killed.");
                    }
                } catch (IOException ex) {
                    throw new ElasticsearchException("failed to stop node", ex);
                } catch (InterruptedException e) {
                    LogManager.getLogger(BootstrapProxy.class).warn("Thread got interrupted while waiting for the node to shutdown.");
                    Thread.currentThread().interrupt();
                }
            }));
        }

        try {
            // look for jar hell
            final Logger logger = LogManager.getLogger(JarHell.class);
            JarHell.checkJarHell(logger::debug);
        } catch (IOException | URISyntaxException e) {
            throw new BootstrapException(e);
        }

        IfConfig.logIfNecessary();

        node = new Node(environment, DEFAULT_PLUGINS, true) {

            @Override
            protected void validateNodeBeforeAcceptingRequests(BoundTransportAddress boundTransportAddress,
                                                               List<BootstrapCheck> bootstrapChecks) throws NodeValidationException {
                BootstrapChecks.check(settings, boundTransportAddress, bootstrapChecks);
            }
        };

    }

    private void start() throws NodeValidationException {
        node.start();
        keepAliveThread.start();
    }

    public static void stop() throws IOException {
        try {
            IOUtils.close(INSTANCE.node);
            if (INSTANCE.node != null && INSTANCE.node.awaitClose(10, TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("Node didn't stop within 10 seconds. Any outstanding requests or tasks might get killed.");
            }
        } catch (InterruptedException e) {
            LogManager.getLogger(BootstrapProxy.class).warn("Thread got interrupted while waiting for the node to shutdown.");
            Thread.currentThread().interrupt();
        } finally {
            INSTANCE.keepAliveLatch.countDown();
        }
    }

    /**
     * This method is invoked by {@link io.crate.bootstrap.CrateDB#main(String[])} to start CrateDB.
     */
    public static void init(Environment environment) throws BootstrapException, NodeValidationException, UserException {
        // force the class initializer for BootstrapInfo to run before
        // the security manager is installed
        BootstrapInfo.init();

        INSTANCE = new BootstrapProxy();
        LogConfigurator.setNodeName(Node.NODE_NAME_SETTING.get(environment.settings()));
        try {
            LogConfigurator.configure(environment);
        } catch (IOException e) {
            throw new BootstrapException(e);
        }
        try {
            // fail if somebody replaced the lucene jars
            checkLucene();

            // install the default uncaught exception handler; must be done before security is
            // initialized as we do not want to grant the runtime permission
            // setDefaultUncaughtExceptionHandler
            Thread.setDefaultUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler());

            INSTANCE.setup(true, environment);

            INSTANCE.start();
        } catch (NodeValidationException | RuntimeException e) {
            // disable console logging, so user does not see the exception twice (jvm will show it already)
            final Logger rootLogger = LogManager.getRootLogger();
            final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
            if (maybeConsoleAppender != null) {
                Loggers.removeAppender(rootLogger, maybeConsoleAppender);
            }
            Logger logger = LogManager.getLogger(BootstrapProxy.class);
            // HACK, it sucks to do this, but we will run users out of disk space otherwise
            if (e instanceof CreationException) {
                // guice: log the shortened exc to the log file
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = null;
                ps = new PrintStream(os, false, StandardCharsets.UTF_8);
                new StartupException(e).printStackTrace(ps);
                ps.flush();
                logger.error("Guice Exception: {}", os.toString(StandardCharsets.UTF_8));
            } else if (e instanceof NodeValidationException) {
                logger.error("node validation exception\n{}", e.getMessage());
            } else {
                // full exception
                logger.error("Exception", e);
            }
            // re-enable it if appropriate, so they can see any logging during the shutdown process
            if (maybeConsoleAppender != null) {
                Loggers.addAppender(rootLogger, maybeConsoleAppender);
            }

            throw e;
        }
    }

    @SuppressForbidden(reason = "System#out")
    private static void closeSystOut() {
        System.out.close();
    }

    @SuppressForbidden(reason = "System#err")
    private static void closeSysError() {
        System.err.close();
    }

    @SuppressForbidden(reason = "Allowed to exit explicitly in bootstrap phase")
    private static void exit(int status) {
        System.exit(status);
    }

    private static void checkLucene() {
        if (org.elasticsearch.Version.CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) == false) {
            throw new AssertionError("Lucene version mismatch this version of CrateDB requires lucene version ["
                                     + org.elasticsearch.Version.CURRENT.luceneVersion + "]  but the current lucene version is [" + org.apache.lucene.util.Version.LATEST + "]");
        }
    }
}

