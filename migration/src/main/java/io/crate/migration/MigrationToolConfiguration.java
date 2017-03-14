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

package io.crate.migration;

import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.MigrationToolSettingsPreparer;

import java.util.List;

/**
 * Holds the necessary configuration for the {@link MigrationTool}
 */
class MigrationToolConfiguration {

    private boolean dryrun;
    private boolean verbose;
    private List<String> tableNames;
    private String configFileName;
    private Environment environment;

    MigrationToolConfiguration(boolean dryrun, boolean verbose, List<String> tableNames, String configFileName) {
        this.dryrun = dryrun;
        this.verbose = verbose;
        this.tableNames = tableNames;
        if (configFileName == null) {
            this.environment = MigrationToolSettingsPreparer.prepareEnvironment();
        } else {
            this.configFileName = configFileName;
            this.environment = MigrationToolSettingsPreparer.prepareEnvironment(configFileName);
        }
    }

    boolean isDryrun() {
        return dryrun;
    }

    boolean isVerbose() {
        return verbose;
    }

    List<String> tableNames() {
        return tableNames;
    }

    String configFileName() {
        return configFileName;
    }

    Environment environment() {
        return environment;
    }
}
