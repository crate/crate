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

package io.crate.upcrater;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Utility class to parse the command line arguments of the {@link Upcrater}
 */
final class UpcraterArgumentParser {

    private static final String DRY_RUN = "--dry-run";
    private static final String CONFIG_FILE_FLAG = "-c";
    private static final String HELP_FLAG = "-h";
    private static final String VERBOSE_FLAG = "--verbose";
    private static final String SPECIFIC_TABLES_FLAG = "--tables";

    private UpcraterArgumentParser() {
    }

    static UpcraterConfiguration parseArgs(String[] args) throws IllegalArgumentException {
        boolean validateOnly = false;
        boolean printHelp = false;
        boolean verbose = false;
        String configFileName = null;
        Set<String> tableNames = new LinkedHashSet<>();

        int i = 0;
        while (i < args.length) {
            String arg = args[i];
            if (HELP_FLAG.equals(arg)) {
                printHelp = true;
                break;
            } else if (DRY_RUN.equals(arg)) {
                validateOnly = true;
            } else if (VERBOSE_FLAG.equals(arg)) {
                verbose = true;
            } else if (CONFIG_FILE_FLAG.equals(arg)) {
                if (i == args.length - 1) {
                    throw new IllegalArgumentException("ERROR: missing value for " + CONFIG_FILE_FLAG + " option.");
                }
                i++;
                configFileName = args[i];
            } else if (SPECIFIC_TABLES_FLAG.equals(arg)) {
                if (i == args.length - 1) {
                    throw new IllegalArgumentException("ERROR: missing value for " + SPECIFIC_TABLES_FLAG + " option.");
                }
                i++;
                StringBuilder sb = new StringBuilder();
                while (i < args.length) {
                    sb.append(args[i]);
                    i++;
                }

                String[] tables = sb.toString().replaceAll("\\s", "").split(",");
                Collections.addAll(tableNames, tables);
            } else {
                throw new IllegalArgumentException("ERROR: Wrong argument provided: " + args[i]);
            }
            i++;
        }

        if (printHelp) {
            return null;
        }
        return new UpcraterConfiguration(validateOnly, verbose, tableNames, configFileName);
    }
}
