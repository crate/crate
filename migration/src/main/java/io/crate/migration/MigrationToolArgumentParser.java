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

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to parse the command line arguments of the {@link MigrationTool}
 */
final class MigrationToolArgumentParser {

    private static final String DRY_RUN = "--dry-run";
    private static final String CONFIG_FILE_FLAG = "-c";
    private static final String VERBOSE_FLAG = "--verbose";
    private static final String ALL_TABLES_FLAG = "--all-tables";
    private static final String SPECIFIC_TABLES_FLAG = "--tables";

    static MigrationToolConfiguration parseArgs(String[] args) throws IllegalArgumentException {
        if (args.length == 0) {
            throw new IllegalArgumentException("ERROR: wrong number of arguments.");
        }

        boolean validateOnly = false;
        boolean allTables = false;
        boolean verbose = false;
        String configFileName = null;
        List<String> tableNames = new ArrayList<>();

        int i = 0;
        while (i < args.length) {
            String arg = args[i];
            if (DRY_RUN.equals(arg)) {
                validateOnly = true;
            } else if (VERBOSE_FLAG.equals(arg)) {
                verbose = true;
            } else if (CONFIG_FILE_FLAG.equals(arg)) {
                if (i == args.length - 1) {
                    throw new IllegalArgumentException("ERROR: missing value for " + CONFIG_FILE_FLAG + " option.");
                }
                i++;
                configFileName = args[i];
            } else if (ALL_TABLES_FLAG.equals(arg)) {
                if (!tableNames.isEmpty()) {
                    throw new IllegalArgumentException("ERROR: Both " + ALL_TABLES_FLAG + " and " +
                                                       SPECIFIC_TABLES_FLAG  + " are specified.");
                }
                allTables = true;
            } else if (SPECIFIC_TABLES_FLAG.equals(arg)) {
                if (allTables) {
                    throw new IllegalArgumentException("ERROR: Both " + ALL_TABLES_FLAG + " and " +
                                                       SPECIFIC_TABLES_FLAG  + " are specified.");
                }
                if (i == args.length - 1) {
                    throw new IllegalArgumentException("ERROR: missing value for " + SPECIFIC_TABLES_FLAG + " option.");
                }
                i++;
                while (i < args.length) {
                    String[] tables = args[i].split(",");
                    for (String table : tables) {
                        tableNames.add(table.replace(",", "").trim());
                    }
                    i++;
                }
            } else {
                throw new IllegalArgumentException("ERROR: Wrong argument provided: " + args[i]);
            }
            i++;
        }

        if (!allTables && tableNames.isEmpty()) {
            throw new IllegalArgumentException("ERROR: None of  or table name(s) are specified.");
        }

        return new MigrationToolConfiguration(validateOnly, verbose, tableNames, configFileName);
    }
}
