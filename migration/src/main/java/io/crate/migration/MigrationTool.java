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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MigrationTool {

    private static final ESLogger LOGGER = Loggers.getLogger(MigrationTool.class);
    final static String USAGE = "Usage:" + System.lineSeparator() +
                         "    java " + MigrationTool.class.getName() +
                         " [-c config.yml] [--dry-run] [--verbose] --all-tables | --tables table1[, table2, ...]" +
                         System.lineSeparator();

    private MigrationTool() {
    }

    public static void main(String[] args) throws IOException {
        MigrationToolConfiguration configuration = null;
        try {
            configuration = MigrationToolArgumentParser.parseArgs(args);
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
                LOGGER.error("Unable to obtain filesystem locks" + e);
                LOGGER.error("Make sure that your cluster is shutdown and there are no running CrateDB instances");
                System.exit(1);
            }

            Map<String, List<File>> indexDirs = new HashMap<>();
            int maxLocalStorageNodes = environment.settings().getAsInt("node.max_local_storage_nodes", 50);
            for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
                for (int dirIndex = 0; dirIndex < environment.dataWithClusterFiles().length; dirIndex++) {
                    Path dir = environment.dataWithClusterFiles()[dirIndex]
                        .resolve(NodeEnvironment.NODES_FOLDER)
                        .resolve(Integer.toString(possibleLockId));
                    retrieveIndexDirs(indexDirs, dir.resolve("indices").toString(), configuration.tableNames());
                }
            }

            for (Map.Entry<String, List<File>> entry : indexDirs.entrySet()) {
                String tableName = entry.getKey();
                List<File> tablePaths = entry.getValue();

                for (File indexDir : tablePaths) {
                    // Validate index directory
                    Path indexPath = indexDir.toPath();

                    if (!Files.isDirectory(indexPath)) {
                        LOGGER.warn("Path [{}] for table [{}] is not valid", indexPath, tableName);
                        continue;
                    }
                    if (!Files.isExecutable(indexPath) && !Files.isExecutable(indexPath)) {
                        LOGGER.warn("Cannot access path [{}] for table [{}] due to invalid permissions",
                            indexPath, tableName);
                        continue;
                    }

                    try {
                        IndexMetaData indexMetaData = IndexMetaDataUtil.loadIndexESMetadata(indexPath);
                        if (indexMetaData == null) {
                            LOGGER.error("Cannot load metadata for [{}] from path [{}]", tableName, indexPath);
                            continue;
                        }

                        if (IndexMetaDataUtil.checkReindexIsRequired(indexMetaData)) {
                            LOGGER.error("Table/partition  [{}] found in [{}] is created before Crate 0.46 " +
                                         "and cannot be upgraded, please reindex instead", tableName, indexPath);
                            continue;
                        }

                        if (IndexMetaDataUtil.checkIndexIsUpgraded(indexMetaData)) {
                            LOGGER.info("Table [{}] found in [{}] is already migrated", tableName, indexPath);
                            continue;
                        }

                        for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                            Path shardIndexPath = indexPath.resolve(String.valueOf(i)).resolve("index");
                            Directory shardDir;
                            try {
                                shardDir = FSDirectory.open(shardIndexPath);
                            } catch (IOException e) {
                                LOGGER.error("Unable to open shard directory [{}] for table [{}]",
                                    shardIndexPath, tableName, e);
                                continue;
                            }

                            if (!IndexMetaDataUtil.checkValidShard(shardDir)) {
                                LOGGER.error("Cannot find a valid shard in directory [{}] for table [{}]",
                                    shardIndexPath, tableName);
                                continue;
                            }

                            if (IndexMetaDataUtil.checkAlreadyMigrated(shardDir)) {
                                LOGGER.info("Shard [{}] in directory [{}] of table [{}] is already migrated",
                                    i, shardIndexPath, tableName);
                                continue;
                            }

                            if (!configuration.isDryrun()) {
                                LOGGER.debug("Migrating shard [{}] in directory [{}] of table [{}]",
                                    i, shardIndexPath, tableName);
                                new IndexUpgrader(shardDir, InfoStream.NO_OUTPUT, true).upgrade();
                                LOGGER.debug("Shard [{}] in directory [{}] of table [{}] migrated",
                                    i, shardIndexPath, tableName);
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error while upgrading table [{}]", tableName, e);
                    }
                }
            }
        }
    }

    @VisibleForTesting
    static void retrieveIndexDirs(Map<String, List<File>> tableDirs, String path, List<String> tableNames) {
        File[] directories = new File(path).listFiles(File::isDirectory);
        if (directories != null) {
            filterIndexPaths(tableDirs, directories, tableNames);
        }
    }

    private static void filterIndexPaths(Map<String, List<File>> tableDirs, File[] dirs, List<String> tableNames) {
        for (String tableName : tableNames) {
            List<File> indexDirs = tableDirs.get(tableName);
            if (indexDirs == null) {
                indexDirs = new ArrayList<>(dirs.length);
            }
            for (File dir : dirs) {
                String patternStr = tableName + "|.*\\.partitioned\\." + tableName + "\\..+";
                if (dir.getName().matches(patternStr)) {
                    indexDirs.add(dir);
                }
            }
            tableDirs.put(tableName, indexDirs);
        }
    }

    private static void printUsage() {
        System.out.print(USAGE);
        System.out.flush();
    }
}
