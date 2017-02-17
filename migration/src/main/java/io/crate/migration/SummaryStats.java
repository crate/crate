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

import java.util.*;

/**
 * Holds information for summary statistics printed at the end of the migration process
 */
class SummaryStats {

    private SortedMap<Table, List<Integer>> successful = new TreeMap<>();
    private SortedMap<Table, List<Integer>> failed = new TreeMap<>();
    private SortedMap<Table, List<Integer>> reindexRequired = new TreeMap<>();
    private SortedMap<Table, List<Integer>> alreadyMigrated = new TreeMap<>();

    void addStatusForTable(Table table, int node, Set<MigrationStatus> statuses) {
        Map<Table, List<Integer>> map = null;
        if (statuses.contains(MigrationStatus.FAILED)) {
            map = failed;
        } else if (statuses.contains(MigrationStatus.REINDEX_REQUIRED)) {
            map = reindexRequired;
        } else if (statuses.contains(MigrationStatus.SUCCESSFUL)) {
            map = successful;
        } else if (statuses.contains(MigrationStatus.ALREADY_MIGRATED)) {
            map = alreadyMigrated;
        }
        assert map != null : "Status must be one of: " + Arrays.toString(MigrationStatus.values());
        List<Integer> nodes = map.computeIfAbsent(table, k -> new ArrayList<>());
        nodes.add(node);
    }

    String print(boolean dryRun) {
        StringBuilder sb = new StringBuilder();
        sb.append(System.lineSeparator());
        sb.append("-------------").append(System.lineSeparator());
        sb.append("-- SUMMARY --").append(System.lineSeparator());
        sb.append("-------------").append(System.lineSeparator()).append(System.lineSeparator());
        if (!reindexRequired.isEmpty()) {
            sb.append("Tables that require reindexing: ");
            appendStats(sb, reindexRequired);
        }
        if (!successful.isEmpty()) {
            if (dryRun) {
                sb.append("Tables to be migrated: ");
            } else {
                sb.append("Tables successfully migrated: ");
            }
            appendStats(sb, successful);
        }
        if (!alreadyMigrated.isEmpty()) {
            sb.append("Tables already migrated: ");
            appendStats(sb, alreadyMigrated);
        }
        if (!failed.isEmpty()) {
            sb.append("Tables errored while migrating (pls check error logs for details): ");
            appendStats(sb, failed);
        }
        return sb.toString();
    }

    private static void appendStats(StringBuilder sb, SortedMap<Table, List<Integer>> map) {
        for (Map.Entry<Table, List<Integer>> entry : map.entrySet()) {
            Table table = entry.getKey();
            List<Integer> nodes = entry.getValue();
            sb.append(table.name()).append('[');
            for (int i = 0; i < nodes.size(); i++) {
                sb.append("node").append(nodes.get(i));
                if (i < nodes.size() - 1) {
                    sb.append(", ");
                } else {
                    sb.append(']');
                }
            }
            sb.append(", ");
        }
        if (sb.lastIndexOf(", ") == sb.length() - 2) {
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(System.lineSeparator());
    }
}
