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

package io.crate.bootstrap;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.BootstrapProxy;
import org.elasticsearch.bootstrap.StartupExceptionProxy;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.NodeNames;
import org.elasticsearch.node.NodeValidationException;

import io.crate.server.cli.EnvironmentAwareCommand;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

/**
 * A main entry point when starting from the command line.
 */
public class CrateDB extends EnvironmentAwareCommand {

    private final OptionSpecBuilder versionOption;

    private CrateDB() {
        super("starts CrateDB", "C", () -> { });
        versionOption = parser.acceptsAll(Arrays.asList("V", "version"),
            "Prints CrateDB version information and exits");
    }

    /**
     * Main entry point for starting crate
     */
    public static void main(final String[] args) throws Exception {
        LogConfigurator.registerErrorListener();
        try (CrateDB crate = new CrateDB()) {
            int status = crate.main(args, Terminal.DEFAULT);
            if (status != ExitCodes.OK) {
                exit(status);
            }
        }
    }

    @Override
    protected Environment createEnv(Map<String, String> cmdLineSettings) throws UserException {
        // 1) Check that path.home is set on the command-line (mandatory)
        String crateHomePath = cmdLineSettings.get("path.home");
        if (crateHomePath == null) {
            throw new IllegalArgumentException("Please set the environment variable CRATE_HOME or " +
                                               "use -Cpath.home on the command-line.");
        }
        // 2) Remove path.conf from command-line settings but use it as a conf path if exists
        //    We need to remove it, because it was removed in ES6, but we want to keep the ability
        //    to set it as CLI argument and keep backwards compatibility.
        String confPathCLI = cmdLineSettings.remove("path.conf");
        final Path confPath;
        if (confPathCLI != null) {
            confPath = Paths.get(confPathCLI);
        } else {
            confPath = Paths.get(crateHomePath, "config");
        }
        return InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, cmdLineSettings, confPath, NodeNames::randomNodeName);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (options.nonOptionArguments().isEmpty() == false) {
            throw new UserException(ExitCodes.USAGE, "Positional arguments not allowed, found " + options.nonOptionArguments());
        }
        if (options.has(versionOption)) {
            terminal.println("Version: " + Version.CURRENT
                             + ", Build: " + Build.CURRENT.hashShort() + "/" + Build.CURRENT.timestamp()
                             + ", JVM: " + JvmInfo.jvmInfo().version());
            return;
        }

        try {
            BootstrapProxy.init(env);
        } catch (BootstrapException | RuntimeException e) {
            // format exceptions to the console in a special way
            // to avoid 2MB stacktraces from guice, etc.
            throw new StartupExceptionProxy(e);
        } catch (NodeValidationException e) {
            throw new UserException(ExitCodes.CONFIG, e.getMessage());
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
        BootstrapProxy.stopInstance();
    }
}
