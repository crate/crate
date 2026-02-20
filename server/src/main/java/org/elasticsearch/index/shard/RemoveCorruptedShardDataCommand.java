/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ElasticsearchNodeCommand;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TruncateTranslog;
import org.jspecify.annotations.Nullable;

import io.crate.common.collections.Tuple;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.util.KeyValuePair;

public class RemoveCorruptedShardDataCommand extends ElasticsearchNodeCommand {

    private static final Logger LOGGER = LogManager.getLogger(RemoveCorruptedShardDataCommand.class);

    private final OptionSpec<String> folderOption;
    private final OptionSpec<String> tableNameOption;
    private final OptionSpec<KeyValuePair> paritionValuesOption;
    private final OptionSpec<Integer> shardIdOption;
    static final String TRUNCATE_CLEAN_TRANSLOG_FLAG = "truncate-clean-translog";

    private final RemoveCorruptedLuceneSegments removeCorruptedLuceneSegments;
    private final TruncateTranslog truncateTranslog;

    public RemoveCorruptedShardDataCommand() {
        super("Removes corrupted shard files");

        folderOption = parser.acceptsAll(Arrays.asList("d", "dir"),
                "Index directory location on disk")
            .withRequiredArg();

        tableNameOption = parser.accepts("table", "Full qualified table name including schema (e.g. `doc.my_table`)")
            .withRequiredArg();

        paritionValuesOption = parser.accepts("P", "Partition values of the partition (e.g. `ts=2026-02-02`)")
            .withOptionalArg()
            .ofType(KeyValuePair.class);

        shardIdOption = parser.accepts("shard-id", "Shard id")
            .withRequiredArg()
            .ofType(Integer.class);

        parser.accepts(TRUNCATE_CLEAN_TRANSLOG_FLAG, "Truncate the translog even if it is not corrupt");

        removeCorruptedLuceneSegments = new RemoveCorruptedLuceneSegments();
        truncateTranslog = new TruncateTranslog();
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("This tool attempts to detect and remove unrecoverable corrupted data in a shard.");
    }

    // Visible for testing
    public OptionParser getParser() {
        return this.parser;
    }

    protected Path getPath(String dirValue) {
        return PathUtils.get(dirValue, "", "");
    }

    protected void findAndProcessShardPath(OptionSet options, Environment environment, Path[] dataPaths, ClusterState clusterState,
                                           CheckedConsumer<ShardPath, IOException> consumer)
        throws IOException {
        final Settings settings = environment.settings();

        final IndexMetadata indexMetadata;
        final int shardId;

        if (options.has(folderOption)) {
            final Path path = getPath(folderOption.value(options)).getParent();
            final Path shardParent = path.getParent();
            final Path shardParentParent = shardParent.getParent();
            final Path indexPath = path.resolve(ShardPath.INDEX_FOLDER_NAME);
            if (Files.exists(indexPath) == false || Files.isDirectory(indexPath) == false) {
                throw new ElasticsearchException("index directory [" + indexPath + "], must exist and be a directory");
            }

            final String shardIdFileName = path.getFileName().toString();
            final String indexUUIDFolderName = shardParent.getFileName().toString();
            if (Files.isDirectory(path) && shardIdFileName.chars().allMatch(Character::isDigit) // SHARD-ID path element check
                && NodeEnvironment.INDICES_FOLDER.equals(shardParentParent.getFileName().toString()) // `indices` check
            ) {
                shardId = Integer.parseInt(shardIdFileName);
                indexMetadata = StreamSupport.stream(clusterState.metadata().indices().values().spliterator(), false)
                    .map(imd -> imd.value)
                    .filter(imd -> imd.getIndexUUID().equals(indexUUIDFolderName)).findFirst()
                    .orElse(null);
            } else {
                throw new ElasticsearchException("Unable to resolve shard id. Wrong folder structure at [ " + path
                    + " ], expected .../indices/[INDEX-UUID]/[SHARD-ID]");
            }
        } else {
            // otherwise resolve shardPath based on the table name, partition and shard id
            String indexName = Objects.requireNonNull(tableNameOption.value(options), "Table name is required");
            List<KeyValuePair> partitionValues = paritionValuesOption.values(options);
            shardId = Objects.requireNonNull(shardIdOption.value(options), "Shard ID is required");
            indexMetadata = resolveIndexMetadata(clusterState, indexName, partitionValues);
        }

        if (indexMetadata == null) {
            throw new ElasticsearchException("Unable to find index in cluster state");
        }

        final IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        final Index index = indexMetadata.getIndex();
        final ShardId shId = new ShardId(index, shardId);

        for (Path dataPath : dataPaths) {
            final Path shardPathLocation = dataPath
                .resolve(NodeEnvironment.INDICES_FOLDER)
                .resolve(index.getUUID())
                .resolve(Integer.toString(shId.id()));
            if (Files.exists(shardPathLocation)) {
                final ShardPath shardPath = ShardPath.loadShardPath(LOGGER, shId, indexSettings.customDataPath(),
                    new Path[]{shardPathLocation}, dataPath);
                if (shardPath != null) {
                    consumer.accept(shardPath);
                    return;
                }
            }
        }
    }

    private static IndexMetadata resolveIndexMetadata(ClusterState clusterState,
                                                      String indexName,
                                                      @Nullable List<KeyValuePair> partitionValues) {
        RelationName relationName = RelationName.fromIndexName(indexName);
        RelationMetadata.Table table = clusterState.metadata().getRelation(relationName);
        if (table == null) {
            throw new ElasticsearchException("Unable to find relation [" + relationName + "] in cluster state");
        }
        List<ColumnIdent> partitionedBy = table.partitionedBy();
        String[] partitionedByValues = new String[partitionedBy.size()];
        if (partitionedBy.isEmpty() == false && partitionValues == null) {
            throw new ElasticsearchException("Partition values must be provided for a partitioned table");
        } else if (partitionValues != null) {
            for (KeyValuePair kvp : partitionValues) {
                ColumnIdent column = ColumnIdent.fromPath(kvp.key);
                int idx = partitionedBy.indexOf(column);
                if (idx < 0) {
                    throw new ElasticsearchException("Column [" + column + "] is not a partitioned by column");
                }
                partitionedByValues[idx] = kvp.value;
            }
        }

        IndexMetadata indexMetadata = clusterState.metadata().getIndex(
            relationName,
            Arrays.asList(partitionedByValues),
            true,
            im -> im
        );
        if (indexMetadata == null) {
            String partitionValString = partitionValues == null ? "" :
                partitionValues.stream()
                    .map(kvp -> kvp.key + "=" + kvp.value)
                    .collect(Collectors.joining(","));
            throw new ElasticsearchException("Unable to find table [" + indexName + "/" + partitionValString + "] in cluster state");
        }
        return indexMetadata;
    }

    public static boolean isCorruptMarkerFileIsPresent(final Directory directory) throws IOException {
        boolean found = false;

        final String[] files = directory.listAll();
        for (String file : files) {
            if (file.startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                found = true;
                break;
            }
        }

        return found;
    }

    protected void dropCorruptMarkerFiles(Terminal terminal, Path path, Directory directory, boolean clean) throws IOException {
        if (clean) {
            confirm("""
                    This shard has been marked as corrupted but no corruption can now be detected.
                    This may indicate an intermittent hardware problem. The corruption marker can be\s
                    removed, but there is a risk that data has been undetectably lost.

                    Are you taking a risk of losing documents and proceed with removing a corrupted marker ?""",
                terminal);
        }
        String[] files = directory.listAll();
        for (String file : files) {
            if (file.startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                directory.deleteFile(file);

                terminal.println("Deleted corrupt marker " + file + " from " + path);
            }
        }
    }

    private static void loseDataDetailsBanner(Terminal terminal, Tuple<CleanStatus, String> cleanStatus) {
        if (cleanStatus.v2() != null) {
            terminal.println("");
            terminal.println("  " + cleanStatus.v2());
            terminal.println("");
        }
    }

    private static void confirm(String msg, Terminal terminal) {
        terminal.println(msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new ElasticsearchException("aborted by user");
        }
    }

    private void warnAboutIndexBackup(Terminal terminal) {
        terminal.println("-----------------------------------------------------------------------");
        terminal.println("");
        terminal.println("  Please make a complete backup of your index before using this tool.");
        terminal.println("");
        terminal.println("-----------------------------------------------------------------------");
    }

    // Visible for testing
    @Override
    public void processNodePaths(Terminal terminal, Path[] dataPaths, OptionSet options, Environment environment) throws IOException {
        warnAboutIndexBackup(terminal);

        final ClusterState clusterState =
            loadTermAndClusterState(createPersistedClusterStateService(environment.settings(), dataPaths), environment).v2();

        findAndProcessShardPath(options, environment, dataPaths, clusterState, shardPath -> {
            final Path indexPath = shardPath.resolveIndex();
            final Path translogPath = shardPath.resolveTranslog();
            if (Files.exists(translogPath) == false || Files.isDirectory(translogPath) == false) {
                throw new ElasticsearchException("translog directory [" + translogPath + "], must exist and be a directory");
            }

            final PrintWriter writer = terminal.getWriter();
            final PrintStream printStream = new PrintStream(new OutputStream() {
                @Override
                public void write(int b) {
                    writer.write(b);
                }
            }, false, StandardCharsets.UTF_8);
            final boolean verbose = terminal.isPrintable(Terminal.Verbosity.VERBOSE);

            final Directory indexDirectory = getDirectory(indexPath);

            final Tuple<CleanStatus, String> indexCleanStatus;
            final Tuple<CleanStatus, String> translogCleanStatus;
            try (Directory indexDir = indexDirectory) {
                // keep the index lock to block any runs of older versions of this tool
                try (Lock writeIndexLock = indexDir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                    // Index
                    terminal.println("");
                    terminal.println("Opening Lucene index at " + indexPath);
                    terminal.println("");
                    try {
                        indexCleanStatus = removeCorruptedLuceneSegments.getCleanStatus(indexDir,
                            writeIndexLock, printStream, verbose);
                    } catch (Exception e) {
                        terminal.println(e.getMessage());
                        throw e;
                    }

                    terminal.println("");
                    terminal.println(" >> Lucene index is " + indexCleanStatus.v1().getMessage() + " at " + indexPath);
                    terminal.println("");

                    // Translog
                    if (options.has(TRUNCATE_CLEAN_TRANSLOG_FLAG)) {
                        translogCleanStatus = new Tuple<>(CleanStatus.OVERRIDDEN,
                            "Translog was not analysed and will be truncated due to the --" + TRUNCATE_CLEAN_TRANSLOG_FLAG + " flag");
                    } else if (indexCleanStatus.v1() != CleanStatus.UNRECOVERABLE) {
                        // translog relies on data stored in an index commit so we have to have a recoverable index to check the translog
                        terminal.println("");
                        terminal.println("Opening translog at " + translogPath);
                        terminal.println("");
                        try {
                            translogCleanStatus = truncateTranslog.getCleanStatus(shardPath, clusterState, indexDir);
                        } catch (Exception e) {
                            terminal.println(e.getMessage());
                            throw e;
                        }

                        terminal.println("");
                        terminal.println(" >> Translog is " + translogCleanStatus.v1().getMessage() + " at " + translogPath);
                        terminal.println("");
                    } else {
                        translogCleanStatus = new Tuple<>(CleanStatus.UNRECOVERABLE, null);
                    }

                    // Drop corrupted data
                    final CleanStatus indexStatus = indexCleanStatus.v1();
                    final CleanStatus translogStatus = translogCleanStatus.v1();

                    if (indexStatus == CleanStatus.CLEAN && translogStatus == CleanStatus.CLEAN) {
                        throw new ElasticsearchException("Shard does not seem to be corrupted at " + shardPath.getDataPath()
                            + " (pass --" + TRUNCATE_CLEAN_TRANSLOG_FLAG + " to truncate the translog anyway)");
                    }

                    if (indexStatus == CleanStatus.UNRECOVERABLE) {
                        if (indexCleanStatus.v2() != null) {
                            terminal.println("Details: " + indexCleanStatus.v2());
                        }

                        terminal.println("You can allocate a new, empty, primary shard with the following command:");

                        printRerouteCommand(shardPath, clusterState, terminal, false);

                        throw new ElasticsearchException("Index is unrecoverable");
                    }


                    terminal.println("-----------------------------------------------------------------------");
                    if (indexStatus != CleanStatus.CLEAN) {
                        loseDataDetailsBanner(terminal, indexCleanStatus);
                    }
                    if (translogStatus != CleanStatus.CLEAN) {
                        loseDataDetailsBanner(terminal, translogCleanStatus);
                    }
                    terminal.println("            WARNING:              YOU MAY LOSE DATA.");
                    terminal.println("-----------------------------------------------------------------------");


                    confirm("Continue and remove corrupted data from the shard ?", terminal);

                    if (indexStatus != CleanStatus.CLEAN) {
                        removeCorruptedLuceneSegments.execute(terminal, indexDir,
                            writeIndexLock, printStream, verbose);
                    }

                    if (translogStatus != CleanStatus.CLEAN) {
                        truncateTranslog.execute(terminal, shardPath, indexDir);
                    }
                } catch (LockObtainFailedException lofe) {
                    final String msg = "Failed to lock shard's directory at [" + indexPath + "], is Elasticsearch still running?";
                    terminal.println(msg);
                    throw new ElasticsearchException(msg);
                }

                final CleanStatus indexStatus = indexCleanStatus.v1();
                final CleanStatus translogStatus = translogCleanStatus.v1();

                // newHistoryCommit obtains its own lock
                addNewHistoryCommit(indexDir, terminal, translogStatus != CleanStatus.CLEAN);
                newAllocationId(shardPath, clusterState, terminal);
                if (indexStatus != CleanStatus.CLEAN) {
                    dropCorruptMarkerFiles(terminal, indexPath, indexDir, indexStatus == CleanStatus.CLEAN_WITH_CORRUPTED_MARKER);
                }
            }
        });
    }

    private Directory getDirectory(Path indexPath) {
        Directory directory;
        try {
            directory = FSDirectory.open(indexPath, NativeFSLockFactory.INSTANCE);
        } catch (Throwable t) {
            throw new ElasticsearchException("ERROR: could not open directory \"" + indexPath + "\"; exiting");
        }
        return directory;
    }

    protected void addNewHistoryCommit(Directory indexDirectory, Terminal terminal, boolean updateLocalCheckpoint) throws IOException {
        final String historyUUID = UUIDs.randomBase64UUID();

        terminal.println("Marking index with the new history uuid : " + historyUUID);
        // commit the new history id
        final IndexWriterConfig iwc = new IndexWriterConfig(null)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setCommitOnClose(false)
            .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        // IndexWriter acquires directory lock by its own
        try (IndexWriter indexWriter = new IndexWriter(indexDirectory, iwc)) {
            final Map<String, String> userData = new HashMap<>();
            indexWriter.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));

            if (updateLocalCheckpoint) {
                // In order to have a safe commit invariant, we have to assign the global checkpoint to the max_seqno of the last commit.
                // We can only safely do it because we will generate a new history uuid this shard.
                final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userData.entrySet());
                // Also advances the local checkpoint of the last commit to its max_seqno.
                userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(commitInfo.maxSeqNo));
            }

            // commit the new history id
            userData.put(Engine.HISTORY_UUID_KEY, historyUUID);

            indexWriter.setLiveCommitData(userData.entrySet());
            indexWriter.commit();
        }
    }

    private void newAllocationId(ShardPath shardPath, ClusterState clusterState, Terminal terminal) throws IOException {
        final Path shardStatePath = shardPath.getShardStatePath();
        final ShardStateMetadata shardStateMetadata =
            ShardStateMetadata.FORMAT.loadLatestState(LOGGER, NAMED_WRITABLE_CONTENT_REGISTRY, NAMED_X_CONTENT_REGISTRY, shardStatePath);

        if (shardStateMetadata == null) {
            throw new ElasticsearchException("No shard state meta data at " + shardStatePath);
        }

        final AllocationId oldAllocationId = shardStateMetadata.allocationId;
        if (oldAllocationId == null) {
            throw new ElasticsearchException("Shard state meta data at " + shardStatePath + " has no allocation id");
        }
        final AllocationId newAllocationId = AllocationId.newInitializing();

        terminal.println("Changing allocation id " + shardStateMetadata.allocationId.getId()
            + " to " + newAllocationId.getId());

        final ShardStateMetadata newShardStateMetadata =
            new ShardStateMetadata(shardStateMetadata.primary, shardStateMetadata.indexUUID, newAllocationId);

        ShardStateMetadata.FORMAT.writeAndCleanup(newShardStateMetadata, shardStatePath);

        terminal.println("");
        terminal.println("You should run the following command to allocate this shard:");

        printRerouteCommand(shardPath, clusterState, terminal, true);
    }

    private void printRerouteCommand(ShardPath shardPath, ClusterState clusterState, Terminal terminal, boolean allocateStale)
        throws IOException {
        final Path nodePath = getNodePath(shardPath);
        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePath);

        if (nodeMetadata == null) {
            throw new ElasticsearchException("No node meta data at " + nodePath);
        }

        final String nodeId = nodeMetadata.nodeId();
        final String indexUUID = shardPath.getShardId().getIndexUUID();
        RelationMetadata relation = clusterState.metadata().getRelation(indexUUID);
        RelationMetadata.Table table;
        String partition = "";
        if (relation instanceof RelationMetadata.Table table1) {
            table = table1;
        } else {
            throw new ElasticsearchException("Relation of index uuid " + indexUUID + " is not a table");
        }

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexUUID);
        if (indexMetadata == null) {
            throw new ElasticsearchException("No index metadata for index uuid " + indexUUID);
        }
        if (table.partitionedBy().isEmpty() == false) {
            partition = partition + " PARTITION (";
            List<String> partitionIdent = new ArrayList<>();
            for (int idx = 0; idx < table.partitionedBy().size(); idx++) {
                ColumnIdent columnIdent = table.partitionedBy().get(idx);
                String value = indexMetadata.partitionValues().get(idx);
                partitionIdent.add(columnIdent.sqlFqn() + " = " + value);
            }
            partition = partition + String.join(",", partitionIdent) + ")";
        }

        final int id = shardPath.getShardId().id();

        terminal.println("");
        terminal.println("ALTER TABLE " + relation.name().sqlFqn() + partition + " REROUTE ALLOCATE " + (allocateStale ? "STALE" : "EMPTY")
            + " PRIMARY SHARD " + id + " ON '" + nodeId + "' WITH (accept_data_loss = true);");
        terminal.println("");
        terminal.println("You must accept the possibility of data loss by changing the `accept_data_loss` parameter to `true`.");
        terminal.println("");
    }

    private Path getNodePath(ShardPath shardPath) {
        final Path nodePath = shardPath.getDataPath().getParent().getParent().getParent();
        if (Files.exists(nodePath) == false ||
            Files.exists(nodePath.resolve(PersistedClusterStateService.METADATA_DIRECTORY_NAME)) == false) {
            throw new ElasticsearchException("Unable to resolve node path for " + shardPath);
        }
        return nodePath;
    }

    public enum CleanStatus {
        CLEAN("clean"),
        CLEAN_WITH_CORRUPTED_MARKER("marked corrupted, but no corruption detected"),
        CORRUPTED("corrupted"),
        UNRECOVERABLE("corrupted and unrecoverable"),
        OVERRIDDEN("to be truncated regardless of whether it is corrupt");

        private final String msg;

        CleanStatus(String msg) {
            this.msg = msg;
        }

        public String getMessage() {
            return msg;
        }
    }

}

