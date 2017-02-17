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

    private Map<Table, List<Integer>> successful = new HashMap<>();
    private Map<Table, List<Integer>> failed = new HashMap<>();
    private Map<Table, List<Integer>> reindexRequired = new HashMap<>();

    void addSuccessFull(Table table, int node) {
        addToList(table, node, Status.SUCCESSFUL);
    }

    void addFailed(Table table, int node) {
        addToList(table, node, Status.FAILED);
    }

    void addReindexRequired(Table table, int node) {
        addToList(table, node, Status.REINDEX_REQUIRED);
    }

    String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("Priting summary statistics...").append(System.lineSeparator());
        sb.append("-- SUMMARY --").append(System.lineSeparator()).append(System.lineSeparator());
        if (!failed.isEmpty()) {
            sb.append("Tables failed to migrate:").append(System.lineSeparator());
            appendStats(sb, failed);
        }
        if (!reindexRequired.isEmpty()) {
            sb.append("Tables that require reindexing:").append(System.lineSeparator());
            appendStats(sb, reindexRequired);
        }
        if (!successful.isEmpty()) {
            sb.append("Tables successfully migrated:").append(System.lineSeparator());
            appendStats(sb, successful);
        }
        return sb.toString();
    }

    private static void appendStats(StringBuilder sb, Map<Table, List<Integer>> map) {
        for (Map.Entry<Table, List<Integer>> entry : map.entrySet()) {
            Table table = entry.getKey();
            List<Integer> nodes = entry.getValue();
            sb.append("    ");
            if (table.isPartitioned()) {
                sb.append("partitioned table [");
            } else {
                sb.append("table [");
            }
            sb.append(table.name()).append("] for nodes: [");
            for (int i = 0; i < nodes.size(); i++) {
                sb.append("node").append(nodes.get(i));
                if (i < nodes.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append(System.lineSeparator());
        }
        sb.append(System.lineSeparator());
    }

    private void addToList(Table table, int node, Status status) {
        Map<Table, List<Integer>> map = null;
        switch (status) {
            case SUCCESSFUL:
                map = successful;
                break;
            case FAILED:
                map = failed;
                break;
            case REINDEX_REQUIRED:
                map = reindexRequired;
                break;
        }
        assert map != null : "Status must be one of: " + Arrays.toString(Status.values());
        List<Integer> nodes = map.computeIfAbsent(table, k -> new ArrayList<>());
        nodes.add(node);
    }

    private enum Status {
        SUCCESSFUL,
        FAILED,
        REINDEX_REQUIRED
    }
}
