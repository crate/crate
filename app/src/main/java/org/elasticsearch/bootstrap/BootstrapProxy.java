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

import org.elasticsearch.common.SuppressForbidden;

public class BootstrapProxy {
    public static void stop() {
        Bootstrap.stop();
    }

    public static void init(String[] args) throws Throwable {
        checkForCustomConfFile();
        Bootstrap.init(args);
    }

    @SuppressForbidden(reason = "System#err")
    private static void sysError(String line, boolean flush) {
        System.err.println(line);
        if (flush) {
            System.err.flush();
        }
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
            sysError(String.format(
                    "{} is no longer supported. crate.yml must be placed in the config directory and cannot be renamed.",
                    settingName), true);
            System.exit(1);
        }
    }
}
