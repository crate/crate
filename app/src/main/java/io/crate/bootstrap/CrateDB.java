/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.bootstrap;

import io.crate.Build;
import io.crate.Version;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import joptsimple.util.PathConverter;
import org.elasticsearch.bootstrap.BootstrapProxy;
import org.elasticsearch.bootstrap.StartupExceptionProxy;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.CrateSettingsPreparer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

/**
 * A main entry point when starting from the command line.
 */
public class CrateDB extends EnvironmentAwareCommand {

    private final OptionSpecBuilder versionOption;
    private final OptionSpecBuilder daemonizeOption;
    private final OptionSpec<Path> pidfileOption;
    private final OptionSpecBuilder quietOption;

    private CrateDB() {
        super("starts CrateDB", "C");
        versionOption = parser.acceptsAll(Arrays.asList("V", "version"),
            "Prints CrateDB version information and exits");
        daemonizeOption = parser.acceptsAll(Arrays.asList("d", "daemonize"),
            "Starts CrateDB in the background")
            .availableUnless(versionOption);
        pidfileOption = parser.acceptsAll(Arrays.asList("p", "pidfile"),
            "Creates a pid file in the specified path on start")
            .availableUnless(versionOption)
            .withRequiredArg()
            .withValuesConvertedBy(new PathConverter());
        quietOption = parser.acceptsAll(Arrays.asList("q", "quiet"),
            "Turns off standard ouput/error streams logging in console")
            .availableUnless(versionOption)
            .availableUnless(daemonizeOption);
    }

    /**
     * Main entry point for starting crate
     */
    public static void main(final String[] args) throws Exception {
        final CrateDB crate = new CrateDB();
        int status = main(args, crate, Terminal.DEFAULT);
        if (status != ExitCodes.OK) {
            exit(status);
        }
    }

    private static int main(final String[] args, final CrateDB crateDb, final Terminal terminal) throws Exception {
        return crateDb.main(args, terminal);
    }

    @Override
    protected Environment createEnv(Terminal terminal, Map<String, String> settings) throws UserException {
        final String esPathConf = System.getProperty("es.path.conf");
        if (esPathConf == null) {
            throw new UserException(ExitCodes.CONFIG, "the system property [es.path.conf] must be set");
        }
        return CrateSettingsPreparer.prepareEnvironment(Settings.EMPTY, settings, Paths.get(esPathConf));
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (options.nonOptionArguments().isEmpty() == false) {
            throw new UserException(ExitCodes.USAGE, "Positional arguments not allowed, found " + options.nonOptionArguments());
        }
        if (options.has(versionOption)) {
            if (options.has(daemonizeOption) || options.has(pidfileOption)) {
                throw new UserException(ExitCodes.USAGE, "CrateDB version option is mutually exclusive with any other option");
            }
            terminal.println("Version: " + Version.CURRENT
                             + ", Build: " + Build.CURRENT.hashShort() + "/" + Build.CURRENT.timestamp()
                             + ", JVM: " + JvmInfo.jvmInfo().version());
            return;
        }

        final boolean daemonize = options.has(daemonizeOption);

        final Path pidFile = pidfileOption.value(options);
        env = addPidFileSetting(pidFile, env);

        final boolean quiet = options.has(quietOption);

        try {
            init(daemonize, quiet, env);
        } catch (NodeValidationException e) {
            throw new UserException(ExitCodes.CONFIG, e.getMessage());
        }
    }

    private static Environment addPidFileSetting(Path pidFile, Environment existingEnv) {
        if (pidFile == null) {
            return existingEnv;
        }
        Settings settingsWithPid = Settings.builder()
            .put(existingEnv.settings())
            .put(Environment.PIDFILE_SETTING.getKey(), pidFile)
            .build();
        return new Environment(settingsWithPid);
    }

    private void init(final boolean daemonize, final boolean quiet, Environment env)
        throws NodeValidationException, UserException {
        try {
            BootstrapProxy.init(!daemonize, quiet, env);
        } catch (BootstrapException | RuntimeException e) {
            // format exceptions to the console in a special way
            // to avoid 2MB stacktraces from guice, etc.
            throw new StartupExceptionProxy(e);
        }
    }

    /**
     * Required method that's called by Apache Commons procrun when
     * running as a service on Windows, when the service is stopped.
     *
     * http://commons.apache.org/proper/commons-daemon/procrun.html
     *
     * NOTE: If this method is renamed and/or moved, make sure to
     * update crate.bat!
     */
    static void close(String[] args) throws IOException {
        BootstrapProxy.stop();
    }
}
