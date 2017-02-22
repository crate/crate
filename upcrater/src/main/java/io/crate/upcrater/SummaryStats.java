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

import java.util.*;

/**
 * Holds information for summary statistics printed at the end of the upgrade process
 */
class SummaryStats {

    private static final String REINDEX_REQUIRED = "Tables that require re-indexing:";
    private static final String UPGRADED_DRYRUN = "Tables to be upgraded:";
    private static final String UPGRADED = "Tables successfully upgraded:";
    private static final String ALREADY_UPGRADED = "Tables already upgraded:";
    private static final String WITH_ERROR = "Tables errored while upgrading (pls check error logs for details):";
    private static final String ALL_DONE = "All is up-to-date. Happy data crunching!";
    private static final String ELEPHANT = "\uD83D\uDC18";

    private SortedMap<Table, List<Integer>> successful = new TreeMap<>();
    private SortedMap<Table, List<Integer>> failed = new TreeMap<>();
    private SortedMap<Table, List<Integer>> reindexRequired = new TreeMap<>();
    private SortedMap<Table, List<Integer>> alreadyUpgraded = new TreeMap<>();

    void addStatusForTable(Table table, int node, Set<UpcrationStatus> statuses) {
        Map<Table, List<Integer>> map = null;
        if (statuses.contains(UpcrationStatus.FAILED)) {
            map = failed;
        } else if (statuses.contains(UpcrationStatus.REINDEX_REQUIRED)) {
            map = reindexRequired;
        } else if (statuses.contains(UpcrationStatus.SUCCESSFUL)) {
            map = successful;
        } else if (statuses.contains(UpcrationStatus.ALREADY_UPGRADED)) {
            map = alreadyUpgraded;
        }
        assert map != null : "Status must be one of: " + Arrays.toString(UpcrationStatus.values());
        List<Integer> nodes = map.computeIfAbsent(table, k -> new ArrayList<>());
        nodes.add(node);
    }

    String print(boolean dryRun) {
        StringJoiner joiner = new StringJoiner(System.lineSeparator());
        joiner.add("");
        joiner.add("-------------");
        joiner.add("-- SUMMARY --");
        joiner.add("-------------");
        joiner.add("");


        String formatString = ELEPHANT + " %-34s %s";
        if (alreadyUpgraded.size() > 0) {
            formatString = ELEPHANT + " %-36s %s";
        }
        if (failed.size() > 0) {
            formatString = ELEPHANT + " %-70s %s";
        }

        boolean allDone = true;
        if (!reindexRequired.isEmpty()) {
            allDone = false;
            joiner.add(String.format(Locale.ENGLISH, formatString, REINDEX_REQUIRED, createTableList(reindexRequired)));
        }
        if (!successful.isEmpty()) {
            String message;
            if (dryRun) {
                message = UPGRADED_DRYRUN;
            } else {
                message = UPGRADED;
            }
            joiner.add(String.format(Locale.ENGLISH, formatString, message, createTableList(successful)));
        }
        if (!alreadyUpgraded.isEmpty()) {
            joiner.add(String.format(Locale.ENGLISH, formatString, ALREADY_UPGRADED, createTableList(alreadyUpgraded)));
        }
        if (!failed.isEmpty()) {
            allDone = false;
            joiner.add(String.format(Locale.ENGLISH, formatString, WITH_ERROR, createTableList(failed)));
        }
        joiner.add("");
        if (allDone) {
            joiner.add(ELEPHANT + "  " + ALL_DONE + "  " + ELEPHANT);
        }
        joiner.add("");
        return joiner.toString();
    }

    private static String createTableList(SortedMap<Table, List<Integer>> map) {
        StringJoiner tableJoiner = new StringJoiner(", ");
        for (Map.Entry<Table, List<Integer>> entry : map.entrySet()) {
            Table table = entry.getKey();
            List<Integer> nodes = entry.getValue();
            StringJoiner nodesJoiner = new StringJoiner(", ");
            for (Integer node : nodes) {
                nodesJoiner.add("node" + node);
            }
            String nodesString = "";
            if (nodes.size() > 1) {
                nodesString = "[" + nodesJoiner.toString() + "]";
            }
            tableJoiner.add(table.name() + nodesString);
        }
        return tableJoiner.toString();
    }
}
