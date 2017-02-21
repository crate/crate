/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.bootstrap;

import io.crate.Version;
import io.crate.node.CrateNode;
import io.crate.bootstrap.BootstrapException;
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
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.PidFile;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.CrateSettingsPreparer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * ES_COPY_OF: core/src/main/java/org/elasticsearch/bootstrap/Bootstrap.java
 * <p>
 * This is a copy of {@link Bootstrap}
 * <p>
 * With following patches:
 * - CrateNode instead of Node is build/started in order to load the CrateCorePlugin
 * - CrateSettingsPreparer is used instead of InternalSettingsPreparer
 * - disabled security manager setup due to policy problems with plugins
 */
public class BootstrapProxy {

    private static volatile BootstrapProxy INSTANCE;

    private volatile CrateNode node;
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    private final Thread keepAliveThread;

    /**
     * creates a new instance
     */
    BootstrapProxy() {
        keepAliveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    keepAliveLatch.await();
                } catch (InterruptedException e) {
                    // bail out
                }
            }
        }, "crate[keepAlive/" + Version.CURRENT + "]");
        keepAliveThread.setDaemon(false);
        // keep this thread alive (non daemon thread) until we shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                keepAliveLatch.countDown();
            }
        });
    }

    /**
     * initialize native resources
     */
    public static void initializeNatives(Path tmpFile, boolean mlockAll, boolean seccomp, boolean ctrlHandler) {
        final Logger logger = Loggers.getLogger(BootstrapProxy.class);

        // check if the user is running as root, and bail
        if (Natives.definitelyRunningAsRoot()) {
            throw new RuntimeException("can not run crate as root");
        }

        // enable secure computing mode
        if (seccomp) {
            Natives.trySeccomp(tmpFile);
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
            Natives.addConsoleCtrlHandler(new ConsoleCtrlHandler() {
                @Override
                public boolean handle(int code) {
                    if (CTRL_CLOSE_EVENT == code) {
                        logger.info("running graceful exit on windows");
                        try {
                            BootstrapProxy.stop();
                        } catch (IOException e) {
                            throw new ElasticsearchException("failed to stop node", e);
                        }
                        return true;
                    }
                    return false;
                }
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
            environment.tmpFile(),
            BootstrapSettings.MEMORY_LOCK_SETTING.get(settings),
            BootstrapSettings.SECCOMP_SETTING.get(settings),
            BootstrapSettings.CTRLHANDLER_SETTING.get(settings));

        // initialize probes before the security manager is installed
        initializeProbes();

        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        IOUtils.close(node);
                        LoggerContext context = (LoggerContext) LogManager.getContext(false);
                        Configurator.shutdown(context);
                    } catch (IOException ex) {
                        throw new ElasticsearchException("failed to stop node", ex);
                    }
                }
            });
        }

        try {
            // look for jar hell
            JarHell.checkJarHell();
        } catch (IOException | URISyntaxException e) {
            throw new BootstrapException(e);
        }

        /**
         * DISABLED setup of security manager due to policy problems with plugins (e.g. SigarPlugin will not work)
         */
        // install SM after natives, shutdown hooks, etc.
        //try {
        //    Security.configure(environment, BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING.get(settings));
        //} catch (IOException | NoSuchAlgorithmException e) {
        //    throw new BootstrapException(e);
        //}

        node = new CrateNode(environment) {
            @Override
            protected void validateNodeBeforeAcceptingRequests(
                final Settings settings,
                final BoundTransportAddress boundTransportAddress) throws NodeValidationException {
                BootstrapCheck.check(settings, boundTransportAddress);
            }
        };

    }

    private static Environment initialEnvironment(boolean foreground, Path pidFile, Map<String, String> esSettings) {
        Terminal terminal = foreground ? Terminal.DEFAULT : null;
        Settings.Builder builder = Settings.builder();
        if (pidFile != null) {
            builder.put(Environment.PIDFILE_SETTING.getKey(), pidFile);
        }
        return CrateSettingsPreparer.prepareEnvironment(builder.build(), terminal, esSettings);
    }

    private void start() throws NodeValidationException {
        node.start();
        keepAliveThread.start();
    }

    public static void stop() throws IOException {
        try {
            IOUtils.close(INSTANCE.node);
        } finally {
            INSTANCE.keepAliveLatch.countDown();
        }
    }

    /** Set the system property before anything has a chance to trigger its use */
    // TODO: why? is it just a bad default somewhere? or is it some BS around 'but the client' garbage <-- my guess
    @SuppressForbidden(reason = "sets logger prefix on initialization")
    static void initLoggerPrefix() {
        System.setProperty("es.logger.prefix", "");
    }

    /**
     * This method is invoked by {@link Elasticsearch#main(String[])}
     * to startup elasticsearch.
     */
    public static void init(final boolean foreground,
                            final Path pidFile,
                            final boolean quiet,
                            final Map<String, String> esSettings) throws BootstrapException, NodeValidationException, UserException {
        // Set the system property before anything has a chance to trigger its use
        initLoggerPrefix();

        // force the class initializer for BootstrapInfo to run before
        // the security manager is installed
        BootstrapInfo.init();

        INSTANCE = new BootstrapProxy();

        Environment environment = initialEnvironment(foreground, pidFile, esSettings);
        try {
            LogConfigurator.configure(environment);
        } catch (IOException e) {
            throw new BootstrapException(e);
        }
        checkForCustomConfFile();

        if (environment.pidFile() != null) {
            try {
                PidFile.create(environment.pidFile(), true);
            } catch (IOException e) {
                throw new BootstrapException(e);
            }
        }

        final boolean closeStandardStreams = (foreground == false) || quiet;
        try {
            if (closeStandardStreams) {
                final Logger rootLogger = ESLoggerFactory.getRootLogger();
                final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
                if (maybeConsoleAppender != null) {
                    Loggers.removeAppender(rootLogger, maybeConsoleAppender);
                }
                closeSystOut();
            }

            // fail if using broken version
            JVMCheck.check();

            // fail if somebody replaced the lucene jars
            checkLucene();

            // install the default uncaught exception handler; must be done before security is
            // initialized as we do not want to grant the runtime permission
            // setDefaultUncaughtExceptionHandler
            Thread.setDefaultUncaughtExceptionHandler(
                new ElasticsearchUncaughtExceptionHandler(() -> Node.NODE_NAME_SETTING.get(environment.settings())));

            INSTANCE.setup(true, environment);

            INSTANCE.start();

            if (closeStandardStreams) {
                closeSysError();
            }
        } catch (NodeValidationException | RuntimeException e) {
            // disable console logging, so user does not see the exception twice (jvm will show it already)
            final Logger rootLogger = ESLoggerFactory.getRootLogger();
            final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
            if (foreground && maybeConsoleAppender != null) {
                Loggers.removeAppender(rootLogger, maybeConsoleAppender);
            }
            Logger logger = Loggers.getLogger(BootstrapProxy.class);
            if (INSTANCE.node != null) {
                logger = Loggers.getLogger(BootstrapProxy.class, Node.NODE_NAME_SETTING.get(INSTANCE.node.settings()));
            }
            // HACK, it sucks to do this, but we will run users out of disk space otherwise
            if (e instanceof CreationException) {
                // guice: log the shortened exc to the log file
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = null;
                try {
                    ps = new PrintStream(os, false, "UTF-8");
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
                new StartupException(e).printStackTrace(ps);
                ps.flush();
                try {
                    logger.error("Guice Exception: {}", os.toString("UTF-8"));
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
            } else if (e instanceof NodeValidationException) {
                logger.error("node validation exception\n{}", e.getMessage());
            } else {
                // full exception
                logger.error("Exception", e);
            }
            // re-enable it if appropriate, so they can see any logging during the shutdown process
            if (foreground && maybeConsoleAppender != null) {
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

    private static void checkForCustomConfFile() {
        String confFileSetting = System.getProperty("es.default.config");
        checkUnsetAndMaybeExit(confFileSetting, "es.default.config");
        confFileSetting = System.getProperty("es.config");
        checkUnsetAndMaybeExit(confFileSetting, "es.config");
        confFileSetting = System.getProperty("elasticsearch.config");
        checkUnsetAndMaybeExit(confFileSetting, "elasticsearch.config");
    }

    private static void checkUnsetAndMaybeExit(String confFileSetting, String settingName) {
        if (confFileSetting != null && confFileSetting.isEmpty() == false) {
            Logger logger = Loggers.getLogger(BootstrapProxy.class);
            logger.info("{} is no longer supported. crate.yml must be placed in the config directory and cannot be renamed.", settingName);
            exit(1);
        }
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

