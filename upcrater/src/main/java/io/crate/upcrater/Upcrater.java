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

import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.operation.reference.sys.check.cluster.IndexMetaDataChecks;
import org.apache.lucene.index.IndexUpgrader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public final class Upcrater {

    private static final ESLogger LOGGER = Loggers.getLogger(Upcrater.class);
    final static String USAGE = "Migration tool to upgrade indexes created with " +
                                "Crate >= 0.46 Crate <= 0.55" + System.lineSeparator() + System.lineSeparator() +
                                "Options:" + System.lineSeparator() +
                                "  [-h] [-c config.yml] [--dry-run] [--verbose] [--tables table1[, table2, ...]]" +
                                System.lineSeparator() + System.lineSeparator() +
                                "Options in detail:" + System.lineSeparator() +
                                "  -h                         print usage information" + System.lineSeparator() +
                                "  -c <filename>              provide an alternative configuration file"
                                + System.lineSeparator() +
                                "  --tables <list_of_tables>  one ore more tables separated with a comma"
                                + System.lineSeparator() +
                                "  --dry-run                  run all the validations but don't upgrade anything"
                                + System.lineSeparator() +
                                "  --verbose                  increase the verbosity level of the tool's output"
                                + System.lineSeparator();

    private Upcrater() {
    }

    public static void main(String[] args) throws IOException {
        UpcraterConfiguration configuration = null;
        try {
            configuration = UpcraterArgumentParser.parseArgs(args);
            if (configuration == null) {
                printUsage();
                System.exit(0);
            }
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.flush();
            printUsage();
            System.exit(1);
        }

        if (configuration.isVerbose()) {
            LOGGER.setLevel("debug");
        }
        Environment environment = configuration.environment();

        try (FSLockUtil fsLockUtil = new FSLockUtil(environment)) {
            try {
                fsLockUtil.obtainLocks();
            } catch (Exception e) {
                LOGGER.error("Unable to obtain filesystem locks.");
                LOGGER.error("Make sure that your cluster is shutdown and there are no running CrateDB instances.", e);
                System.exit(1);
            }

            SummaryStats summaryStats = execute(configuration, environment);
            System.out.flush();
            System.out.print(summaryStats.print(configuration.isDryrun()));
            System.out.flush();
        }
        System.exit(0);
    }

    private static SummaryStats execute(UpcraterConfiguration configuration, Environment environment) {
        SummaryStats summaryStats = new SummaryStats();

        int maxLocalStorageNodes = environment.settings().getAsInt("node.max_local_storage_nodes", 50);

        for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
            for (int node = 0; node < environment.dataWithClusterFiles().length; node++) {
                Path dir = environment.dataWithClusterFiles()[node]
                    .resolve(NodeEnvironment.NODES_FOLDER)
                    .resolve(Integer.toString(possibleLockId));
                Map<Table, List<File>> indexDirs = new HashMap<>();
                retrieveIndexDirs(indexDirs, dir.resolve("indices").toString(), configuration.tableNames());

                for (Map.Entry<Table, List<File>> entry : indexDirs.entrySet()) {
                    Table table = entry.getKey();
                    String tableName = table.name();

                    List<File> tablePaths = entry.getValue();
                    Set<UpcrationStatus> statuses = new HashSet<>();

                    for (File indexDir : tablePaths) {
                        // Validate index directory
                        Path indexPath = indexDir.toPath();

                        if (!Files.isDirectory(indexPath)) {
                            LOGGER.error("Path [{}] for {}table [{}] of node [{}] is not valid",
                                indexPath,
                                table.isPartitioned() ? "partitioned " : "",
                                tableName,
                                possibleLockId);
                            statuses.add(UpcrationStatus.FAILED);
                            continue;
                        }
                        if (!Files.isExecutable(indexPath) && !Files.isExecutable(indexPath)) {
                            LOGGER.error("Cannot access path [{}] for {}table [{}] of node [{}] " +
                                         "due to invalid permissions",
                                indexPath,
                                table.isPartitioned() ? "partitioned " : "",
                                tableName,
                                possibleLockId);
                            statuses.add(UpcrationStatus.FAILED);
                            continue;
                        }

                        try {
                            IndexMetaData indexMetaData = IndexMetaDataChecks.loadIndexESMetadata(indexPath, LOGGER);
                            if (indexMetaData == null) {
                                LOGGER.error("Cannot load metadata for {}table [{}] of [{}] from path [{}]",
                                    table.isPartitioned() ? "partitioned " : "",
                                    tableName,
                                    possibleLockId,
                                    indexPath);
                                statuses.add(UpcrationStatus.FAILED);
                                continue;
                            }

                            if (IndexMetaDataChecks.checkReindexIsRequired(indexMetaData)) {
                                LOGGER.warn("{} [{}] of node [{}] found in path [{}] is created before " + "" +
                                            "Crate 0.46 and cannot be upgraded, please reindex instead",
                                    table.isPartitioned() ? "Partitioned table" : "Table",
                                    tableName,
                                    possibleLockId,
                                    indexPath);
                                statuses.add(UpcrationStatus.REINDEX_REQUIRED);
                                continue;
                            }

                            if (IndexMetaDataChecks.checkIndexIsUpgraded(indexMetaData)) {
                                LOGGER.debug("{} [{}] of node [{}] found in path [{}] is already upgraded",
                                    table.isPartitioned() ? "Partitioned table" : "Table",
                                    tableName,
                                    possibleLockId,
                                    indexPath);
                                statuses.add(UpcrationStatus.ALREADY_UPGRADED);
                                continue;
                            }

                            int numberOfShards = 0;
                            int upgradedShards = 0;
                            int alreadyUpgradedShards = 0;
                            for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                                Path shardIndexPath = indexPath.resolve(String.valueOf(i)).resolve("index");
                                Directory shardDir;
                                if (Files.isDirectory(shardIndexPath)) {
                                    numberOfShards++;
                                    try {
                                        shardDir = FSDirectory.open(shardIndexPath);
                                    } catch (IOException e) {
                                        LOGGER.error("Unable to open shard directory [{}] for {}table [{}]" +
                                                     "of node [{}]",
                                            shardIndexPath,
                                            table.isPartitioned() ? "partitioned " : "",
                                            tableName,
                                            possibleLockId,
                                            e);
                                        statuses.add(UpcrationStatus.FAILED);
                                        continue;
                                    }

                                    if (!IndexMetaDataChecks.checkValidShard(shardDir)) {
                                        LOGGER.error("Cannot find a valid shard in directory [{}] for " +
                                                     "{}table [{}] of node [{}]",
                                            shardIndexPath,
                                            table.isPartitioned() ? "partitioned " : "",
                                            tableName,
                                            possibleLockId);
                                        statuses.add(UpcrationStatus.FAILED);
                                        continue;
                                    }

                                    if (IndexMetaDataChecks.checkAlreadyUpgraded(shardDir)) {
                                        LOGGER.debug("Shard [{}] in directory [{}] for {}table [{}] of node [{}] " +
                                                     "is already upgraded",
                                            i,
                                            shardIndexPath,
                                            table.isPartitioned() ? "partitioned " : "",
                                            tableName,
                                            possibleLockId);
                                        alreadyUpgradedShards++;
                                        continue;
                                    }
                                    if (!configuration.isDryrun()) {
                                        LOGGER.debug("Migrating shard [{}] in directory [{}] for {}table [{}] " +
                                                     "of node [{}]",
                                            i,
                                            shardIndexPath,
                                            table.isPartitioned() ? "partitioned " : "",
                                            tableName,
                                            possibleLockId);
                                        new IndexUpgrader(shardDir, InfoStream.NO_OUTPUT, true).upgrade();
                                        LOGGER.debug("Shard [{}] in directory [{}] for {}table [{}] of node [{}] " +
                                                     "upgraded successfully",
                                            i,
                                            shardIndexPath,
                                            table.isPartitioned() ? "partitioned " : "",
                                            tableName,
                                            possibleLockId);
                                    }
                                    upgradedShards++;
                                }
                            }
                            if (upgradedShards + alreadyUpgradedShards == numberOfShards) {
                                if (upgradedShards > 0) {
                                    statuses.add(UpcrationStatus.SUCCESSFUL);
                                } else {
                                    statuses.add(UpcrationStatus.ALREADY_UPGRADED);
                                }
                            }
                        } catch (Exception e) {
                            LOGGER.error("Error while upgrading {}table [{}] of node [{}]",
                                table.isPartitioned() ? "partitioned " : "",
                                tableName,
                                possibleLockId,
                                e);
                        }
                    }
                    summaryStats.addStatusForTable(table, possibleLockId, statuses);
                }
            }
        }
        return summaryStats;
    }

    @VisibleForTesting
    static void retrieveIndexDirs(Map<Table, List<File>> tableDirs, String path, Set<String> tableNames) {
        File[] directories = new File(path).listFiles(File::isDirectory);
        if (directories != null) {
            filterIndexPaths(tableDirs, directories, tableNames);
        }
    }

    private static void filterIndexPaths(Map<Table, List<File>> tableDirs, File[] dirs, Set<String> tableNames) {
        for (File dir : dirs) {
            String dirName = dir.getName();
            TableIdent tableIdent = TableIdent.fromIndexName(dirName);

            if (tableNames.isEmpty() || tableNames.contains(tableIdent.name())) {
                Table table = new Table(tableIdent, PartitionName.isPartition(dirName));
                List<File> indexDirs = tableDirs.computeIfAbsent(table, k -> new ArrayList<>(dirs.length));
                indexDirs.add(dir);
            }
        }
    }

    private static void printUsage() {
        System.out.flush();
        System.out.print(USAGE);
        System.out.flush();
    }
}
