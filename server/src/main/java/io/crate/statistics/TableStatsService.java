/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.statistics;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;
import io.crate.data.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.session.BaseResultReceiver;
import io.crate.session.Session;
import io.crate.session.Sessions;

/**
 * Handles persistence for {@link TableStats} and periodically refreshes {@link TableStats}
 * based on {@link #refreshInterval}.
 */
@Singleton
public class TableStatsService extends AbstractLifecycleComponent implements Runnable, ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(TableStatsService.class);

    public static final Setting<TimeValue> STATS_SERVICE_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "stats.service.interval", TimeValue.timeValueHours(24), Property.NodeScope, Property.Dynamic, Property.Exposed);

    public static final Setting<ByteSizeValue> STATS_SERVICE_THROTTLING_SETTING = Setting.byteSizeSetting(
        "stats.service.max_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB), Property.NodeScope, Property.Dynamic, Property.Exposed);

    static final String STMT = "ANALYZE";
    private static final String TABLES_STATS = "_stats_tables";
    private static final String COLS_STATS = "_stats_cols";
    private static final String DATA_FIELD = "data";
    private static final String RELATION_NAME_FIELD = "relationName";
    private static final String COLUMN_NAME_FIELD = "columnName";
    private static final String COLUMN_NAMES_FIELD = "columnNames";

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Sessions sessions;
    private final Directory tablesDirectory;
    private final Directory colsDirectory;
    private final IndexWriter tablesWriter;
    private final IndexWriter colsWriter;
    private final SearcherManager tablesSearcherManager;
    private final SearcherManager colsSearcherManager;
    private final LoadingCache<RelationName, Stats> cache;

    private Session session;

    @VisibleForTesting
    volatile TimeValue refreshInterval;

    @VisibleForTesting
    volatile Scheduler.ScheduledCancellable scheduledRefresh;



    public TableStatsService(Settings settings,
                             ThreadPool threadPool,
                             ClusterService clusterService,
                             Sessions sessions,
                             Path dataPath) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.sessions = sessions;
        refreshInterval = STATS_SERVICE_REFRESH_INTERVAL_SETTING.get(settings);
        scheduledRefresh = scheduleNextRefresh(refreshInterval);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            STATS_SERVICE_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);

        cache = Caffeine.newBuilder()
            .executor(threadPool.generic())
            .maximumWeight(10_000)
            .weigher((RelationName _, Stats s) -> s.statsByColumn().size())
            .build(this::loadFromDisk);

        try {
            this.tablesDirectory = new NIOFSDirectory(dataPath.resolve(TABLES_STATS));
            this.colsDirectory = new NIOFSDirectory(dataPath.resolve(COLS_STATS));
            IndexWriterConfig tablesIndexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
            tablesIndexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            tablesIndexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
            IndexWriterConfig colsIndexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
            colsIndexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            colsIndexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
            this.tablesWriter = new IndexWriter(tablesDirectory, tablesIndexWriterConfig);
            this.tablesSearcherManager = new SearcherManager(tablesWriter, null);
            this.colsWriter = new IndexWriter(colsDirectory, colsIndexWriterConfig);
            this.colsSearcherManager = new SearcherManager(colsWriter, null);

            // ensure index files exist for reads
            this.update(Map.of());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() throws IOException {
        cache.invalidateAll();
        IOUtils.closeWhileHandlingException(
            tablesSearcherManager,
            colsSearcherManager,
            tablesWriter,
            colsWriter,
            tablesDirectory,
            colsDirectory);
    }

    @Override
    public void run() {
        updateStats();
    }

    public void updateStats() {
        if (clusterService.localNode() == null) {
            /*
              During a long startup (e.g. during an upgrade process) the localNode() may be null
              and this would lead to NullPointerException in the TransportExecutor.
             */
            LOGGER.debug("Could not retrieve table stats. localNode is not fully available yet.");
            return;
        }
        if (clusterService.state().nodes().getMinNodeVersion().before(Version.V_5_7_0)) {
            // Streaming format changed in 5.7.0, so wait until everything is upgraded before
            // collecting stats
            LOGGER.debug("Could not retrieve table stats.  Cluster not fully updated yet");
            return;
        }
        if (!clusterService.state().nodes().isLocalNodeElectedMaster()) {
            // `ANALYZE` will publish the new stats to all nodes, so we need only a single node running it.
            return;
        }
        try {
            BaseResultReceiver resultReceiver = new BaseResultReceiver();
            resultReceiver.completionFuture().whenComplete((_, err) -> {
                scheduledRefresh = scheduleNextRefresh(refreshInterval);
                if (err != null) {
                    LOGGER.error("Error running periodic " + STMT, err);
                }
            });
            if (session == null) {
                session = sessions.newSystemSession();
            }
            session.quickExec(STMT, resultReceiver, Row.EMPTY);
        } catch (Throwable t) {
            LOGGER.error("error retrieving table stats", t);
        }
    }

    @Nullable
    private Scheduler.ScheduledCancellable scheduleNextRefresh(TimeValue refreshInterval) {
        if (refreshInterval.millis() > 0) {
            return threadPool.schedule(
                this,
                refreshInterval,
                ThreadPool.Names.REFRESH
            );
        }
        return null;
    }

    private void setRefreshInterval(TimeValue newRefreshInterval) {
        if (scheduledRefresh != null) {
            scheduledRefresh.cancel();
            scheduledRefresh = null;
        }
        refreshInterval = newRefreshInterval;
        scheduledRefresh = scheduleNextRefresh(newRefreshInterval);
    }

    @Nullable
    public Stats get(RelationName relationName) {
        return cache.get(relationName);
    }

    public void remove(RelationName relationName) {
        try {
            PersistedTable persistedTable = loadTableStats(relationName);
            if (persistedTable != null) {
                for (ColumnIdent columnIdent : persistedTable.cols()) {
                    colsWriter.deleteDocuments(new TermQuery(colTerm(relationName, columnIdent)));
                }
                colsWriter.commit();
                tablesWriter.deleteDocuments(relTerm(relationName));
                tablesWriter.commit();
            }
            cache.invalidate(relationName);
        } catch (IOException e) {
            throw new UncheckedIOException("Can't delete TableStats from disk", e);
        }
    }

    public void clear() {
        try {
            tablesWriter.deleteAll();
            tablesWriter.commit();
            colsWriter.deleteAll();
            colsWriter.commit();
            cache.invalidateAll();
        } catch (IOException e) {
            throw new UncheckedIOException("Can't delete TableStats from disk", e);
        }
    }

    @VisibleForTesting
    @Nullable
    Stats loadFromDisk(RelationName relationName) {
        PersistedTable persistedTable = loadTableStats(relationName);
        if (persistedTable == null) {
            return null;
        }
        Map<ColumnIdent, ColumnStats<?>> columnStatsMap = loadColumnStats(relationName, persistedTable.cols());
        return new Stats(persistedTable.numDocs(), persistedTable.sizeInBytes(), columnStatsMap);
    }

    public void update(Map<RelationName, Stats> stats) {
        try {
            for (Map.Entry<RelationName, Stats> entry : stats.entrySet()) {
                RelationName relationName = entry.getKey();
                DocsToPersist docs = makeDocument(relationName, entry.getValue());
                tablesWriter.updateDocument(relTerm(relationName), docs.table());
                for (Map.Entry<ColumnIdent, Document> docsEntry : docs.cols().entrySet()) {
                    colsWriter.updateDocument(colTerm(relationName, docsEntry.getKey()), docsEntry.getValue());
                }
            }
            tablesWriter.commit();
            colsWriter.commit();
            tablesSearcherManager.maybeRefresh();
            colsSearcherManager.maybeRefresh();
            cache.invalidateAll(stats.keySet());
        } catch (IOException e) {
            throw new UncheckedIOException("Can't write TableStats to disk", e);
        }
    }

    private static Term relTerm(RelationName relationName) {
        return new Term(RELATION_NAME_FIELD, relationName.fqn());
    }

    private static Term colTerm(RelationName relationName, ColumnIdent columnIdent) {
        return new Term(COLUMN_NAME_FIELD, relationName.fqn() + "." + columnIdent.sqlFqn());
    }

    private static DocsToPersist makeDocument(RelationName relationName, Stats stats) throws IOException {
        // table stats
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        Version.writeVersion(Version.CURRENT, bytesStreamOutput);
        bytesStreamOutput.writeVLong(stats.numDocs());
        bytesStreamOutput.writeVLong(stats.sizeInBytes());
        Document tableDocument = new Document();
        tableDocument.add(new StringField(RELATION_NAME_FIELD, relationName.fqn(), Field.Store.NO));
        tableDocument.add(new StoredField(DATA_FIELD, bytesStreamOutput.bytes().toBytesRef()));

        // column stats
        Map<ColumnIdent, Document> colDocuments = HashMap.newHashMap(stats.statsByColumn().size());
        for (var entry : stats.statsByColumn().entrySet()) {
            String colName = entry.getKey().sqlFqn();
            // Add the column name to table doc
            tableDocument.add(new StringField(COLUMN_NAMES_FIELD, colName, Field.Store.YES));

            Document colDocument = new Document();
            bytesStreamOutput = new BytesStreamOutput();
            entry.getValue().writeTo(bytesStreamOutput);
            colDocument.add(new StringField(COLUMN_NAME_FIELD, relationName.fqn() + "." + colName, Field.Store.NO));
            colDocument.add(new StoredField(DATA_FIELD, bytesStreamOutput.bytes().toBytesRef()));
            colDocuments.put(entry.getKey(), colDocument);
        }
        return new DocsToPersist(tableDocument, colDocuments);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metadataChanged() || event.isNewCluster()) {
            return;
        }

        Metadata oldMetadata = event.previousState().metadata();
        Metadata newMetadata = event.state().metadata();
        for (var oldRelation : oldMetadata.relations(RelationMetadata.class)) {
            RelationName name = oldRelation.name();
            if (!newMetadata.contains(name)) {
                remove(name);
            }
        }
    }

    private PersistedTable loadTableStats(RelationName relationName) {
        try {
            PersistedTable persistedTable = null;
            IndexSearcher tablesSearcher = null;
            try {
                tablesSearcherManager.maybeRefreshBlocking();
                tablesSearcher = tablesSearcherManager.acquire();
                tablesSearcher.setQueryCache(null);
                Query query = new TermQuery(relTerm(relationName));
                Weight weight = tablesSearcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
                IndexReader reader = tablesSearcher.getIndexReader();
                for (LeafReaderContext leafReaderContext : reader.leaves()) {
                    Scorer scorer = weight.scorer(leafReaderContext);
                    if (scorer == null) {
                        continue;
                    }
                    DocIdSetIterator docIdSetIterator = scorer.iterator();
                    StoredFields storedFields = leafReaderContext.reader().storedFields();
                    if (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        Document doc = storedFields.document(docIdSetIterator.docID());
                        BytesRef binaryValue = doc.getBinaryValue(DATA_FIELD);
                        ByteArrayInputStream bis = new ByteArrayInputStream(binaryValue.bytes);
                        InputStreamStreamInput in = new InputStreamStreamInput(bis);
                        Version version = Version.readVersion(in);
                        in.setVersion(version);
                        long numDocs = in.readVLong();
                        long sizeInBytes = in.readVLong();
                        String[] colNames = doc.getValues(COLUMN_NAMES_FIELD);
                        List<ColumnIdent> cols = new ArrayList<>(colNames.length);
                        for (String colName : colNames) {
                            cols.add(ColumnIdent.of(colName));
                        }
                        persistedTable = new PersistedTable(relationName, numDocs, sizeInBytes, cols);
                        break;
                    }
                }
            } finally {
                tablesSearcherManager.release(tablesSearcher);
            }
            return persistedTable;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @VisibleForTesting
    Map<ColumnIdent, ColumnStats<?>> loadColumnStats(RelationName relationName, List<ColumnIdent> cols) {
        Map<ColumnIdent, ColumnStats<?>> columnStatsMap = new HashMap<>();
        for (ColumnIdent columnIdent : cols) {
            try {
                IndexSearcher searcher = null;
                try {
                    colsSearcherManager.maybeRefreshBlocking();
                    searcher = colsSearcherManager.acquire();
                    searcher.setQueryCache(null);
                    Query query = new TermQuery(colTerm(relationName, columnIdent));
                    Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
                    IndexReader reader = searcher.getIndexReader();
                    for (LeafReaderContext leafReaderContext : reader.leaves()) {
                        Scorer scorer = weight.scorer(leafReaderContext);
                        if (scorer == null) {
                            continue;
                        }
                        DocIdSetIterator docIdSetIterator = scorer.iterator();
                        StoredFields storedFields = leafReaderContext.reader().storedFields();
                        if (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            Document colsDoc = storedFields.document(docIdSetIterator.docID());
                            BytesRef colsBinaryValue = colsDoc.getBinaryValue(DATA_FIELD);
                            ByteArrayInputStream colsBis = new ByteArrayInputStream(colsBinaryValue.bytes);
                            InputStreamStreamInput colsIn = new InputStreamStreamInput(colsBis);
                            ColumnStats<?> columnStats = new ColumnStats<>(colsIn);
                            columnStatsMap.put(columnIdent, columnStats);
                        }
                    }
                } finally {
                    colsSearcherManager.release(searcher);
                }

            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        return columnStatsMap;
    }

    private record DocsToPersist(Document table, Map<ColumnIdent, Document> cols){}

    private record PersistedTable(RelationName relationName, long numDocs, long sizeInBytes, List<ColumnIdent> cols){}
}

