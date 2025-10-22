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


import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
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
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
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

import io.crate.Streamer;
import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;
import io.crate.data.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.session.BaseResultReceiver;
import io.crate.session.Session;
import io.crate.session.Sessions;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

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

    private static class MetaDocFields {
        static final String VERSION = "version";
    }

    private static class TableDocFields {

        static final String NAME = "relation";
        static final String NUM_DOCS = "numDocs";
        static final String SIZE_IN_BYTES = "size";
    }

    private static class ColumnDocFields {

        private static final String REL_NAME = "relation";
        private static final String COLUMN = "column";
        private static final String VALUE_TYPE = "valueType";
        private static final String NULL_FRACTION = "nullFraction";
        private static final String AVG_SIZE_IN_BYTES = "avgSize";
        private static final String APPROX_DISTINCT = "approxDistinct";
        private static final String MOST_COMMON_VALUES = "mcv";
        private static final String HISTOGRAM = "histogram";
    }

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
            this.tablesDirectory = MMapDirectory.open(dataPath.resolve(TABLES_STATS));
            this.colsDirectory = MMapDirectory.open(dataPath.resolve(COLS_STATS));
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
            String relNameFqn = relationName.fqn();
            tablesWriter.deleteDocuments(new Term(TableDocFields.NAME, relNameFqn));
            colsWriter.deleteDocuments(new Term(ColumnDocFields.REL_NAME, relNameFqn));
            tablesWriter.commit();
            colsWriter.commit();
            cache.invalidate(relationName);
        } catch (IOException e) {
            throw new UncheckedIOException("Can't delete TableStats from disk", e);
        }
    }

    @VisibleForTesting
    public void clear() {
        try {
            tablesWriter.deleteAll();
            colsWriter.deleteAll();
            tablesWriter.commit();
            colsWriter.commit();
            cache.invalidateAll();
        } catch (IOException e) {
            throw new UncheckedIOException("Can't delete TableStats from disk", e);
        }
    }

    @VisibleForTesting
    @Nullable
    Stats loadFromDisk(RelationName relationName) {
        String relNameFqn = relationName.fqn();
        HashMap<ColumnIdent, ColumnStats<?>> columnStatsMap = new HashMap<>();
        long numDocs = 0;
        long sizeInBytes = 0;
        IndexSearcher searcher = null;
        boolean match = false;
        try {
            tablesSearcherManager.maybeRefreshBlocking();
            searcher = tablesSearcherManager.acquire();
            searcher.setQueryCache(null);
            Query query = new TermQuery(new Term(TableDocFields.NAME, relNameFqn));
            Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
            IndexReader reader = searcher.getIndexReader();
            TopDocs versionTopDoc = searcher.search(new FieldExistsQuery(MetaDocFields.VERSION), 1);
            if (versionTopDoc.totalHits.value() != 1L) {
                return null;
            }
            int versionDocId = versionTopDoc.scoreDocs[0].doc;
            Document versionDoc = reader.storedFields().document(versionDocId);
            int internalVersion = versionDoc.getField(MetaDocFields.VERSION).storedValue().getIntValue();
            Version version = Version.fromId(internalVersion);
            for (LeafReaderContext leafReaderContext : reader.leaves()) {
                Scorer scorer = weight.scorer(leafReaderContext);
                if (scorer == null) {
                    continue;
                }
                DocIdSetIterator docIdSetIterator = scorer.iterator();
                StoredFields storedFields = leafReaderContext.reader().storedFields();
                while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    match = true;
                    Document doc = storedFields.document(docIdSetIterator.docID());
                    numDocs = doc.getField(TableDocFields.NUM_DOCS).storedValue().getLongValue();
                    sizeInBytes = doc.getField(TableDocFields.SIZE_IN_BYTES).storedValue().getLongValue();
                }
                loadColStatsFromDisk(relNameFqn, columnStatsMap, version);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Can't load TableStats from disk", ex);
        } finally {
            try {
                tablesSearcherManager.release(searcher);
            } catch (IOException ex) {
                throw new UncheckedIOException("Can't load TableStats from disk", ex);
            }
        }
        if (!match) {
            return null;
        }
        return new Stats(numDocs, sizeInBytes, columnStatsMap);
    }

    private void loadColStatsFromDisk(String relNameFqn,
                                      Map<ColumnIdent, ColumnStats<?>> columnStatsMap,
                                      Version version) {
        IndexSearcher searcher = null;
        try {
            colsSearcherManager.maybeRefreshBlocking();
            searcher = colsSearcherManager.acquire();
            searcher.setQueryCache(null);
            Query query = new TermQuery(new Term(ColumnDocFields.REL_NAME, relNameFqn));
            Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
            IndexReader reader = searcher.getIndexReader();
            for (LeafReaderContext leafReaderContext : reader.leaves()) {
                Scorer scorer = weight.scorer(leafReaderContext);
                if (scorer == null) {
                    continue;
                }
                DocIdSetIterator docIdSetIterator = scorer.iterator();
                StoredFields storedFields = leafReaderContext.reader().storedFields();
                while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    Document doc = storedFields.document(docIdSetIterator.docID());
                    String columnName = doc.getField(ColumnDocFields.COLUMN).stringValue();
                    ColumnIdent column = ColumnIdent.of(columnName);
                    DataType<?> valueType = decode(
                        version,
                        DataTypes::fromStream,
                        doc.getBinaryValue(ColumnDocFields.VALUE_TYPE)
                    );
                    columnStatsMap.put(column, readColumnStats(version, doc, valueType));
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Can't load TableStats from disk", ex);
        } finally {
            try {
                colsSearcherManager.release(searcher);
            } catch (IOException ex) {
                throw new UncheckedIOException("Can't load TableStats from disk", ex);
            }
        }
    }

    private static <T> ColumnStats<T> readColumnStats(Version version, Document doc, DataType<T> type) throws IOException {
        double nullFraction = doc.getField(ColumnDocFields.NULL_FRACTION).storedValue().getDoubleValue();
        double avgSizeInBytes = doc.getField(ColumnDocFields.AVG_SIZE_IN_BYTES).storedValue().getDoubleValue();
        double approxDistinct = doc.getField(ColumnDocFields.APPROX_DISTINCT).storedValue().getDoubleValue();
        Streamer<T> streamer = type.streamer();
        MostCommonValues<T> mostCommonValues = decode(
            version,
            in -> new MostCommonValues<>(streamer, in),
            doc.getBinaryValue(ColumnDocFields.MOST_COMMON_VALUES)
        );
        List<T> histogram = decode(
            version,
            in -> in.readList(streamer::readValueFrom),
            doc.getBinaryValue(ColumnDocFields.HISTOGRAM)
        );
        return new ColumnStats<>(
            nullFraction,
            avgSizeInBytes,
            approxDistinct,
            type,
            mostCommonValues,
            histogram
        );
    }

    public void update(Map<RelationName, Stats> relationsStats) {
        try {
            tablesWriter.deleteAll();
            colsWriter.deleteAll();
            Document metaDoc = new Document();
            metaDoc.add(new IntField(MetaDocFields.VERSION, Version.CURRENT.internalId, Field.Store.YES));
            tablesWriter.addDocument(metaDoc);
            for (var entry : relationsStats.entrySet()) {
                RelationName relationName = entry.getKey();
                Stats stats = entry.getValue();

                Document tableDoc = new Document();
                String fqTableName = relationName.fqn();
                tableDoc.add(new StringField(TableDocFields.NAME, fqTableName, Field.Store.NO));
                tableDoc.add(new StoredField(TableDocFields.NUM_DOCS, stats.numDocs()));
                tableDoc.add(new StoredField(TableDocFields.SIZE_IN_BYTES, stats.sizeInBytes()));
                tablesWriter.addDocument(tableDoc);

                for (var columnEntry : stats.statsByColumn().entrySet()) {
                    ColumnIdent column = columnEntry.getKey();
                    ColumnStats<?> columnStats = columnEntry.getValue();
                    colsWriter.addDocument(createColDoc(fqTableName, column, columnStats));
                }
            }
            tablesWriter.commit();
            colsWriter.commit();
            tablesSearcherManager.maybeRefresh();
            colsSearcherManager.maybeRefresh();
            cache.invalidateAll(relationsStats.keySet());
        } catch (IOException e) {
            throw new UncheckedIOException("Can't write TableStats to disk", e);
        }
    }

    private static <T> Document createColDoc(String fqTableName, ColumnIdent column, ColumnStats<T> columnStats) throws IOException {
        String sqlFqn = column.sqlFqn();
        Document colDoc = new Document();
        colDoc.add(new StringField(ColumnDocFields.REL_NAME, fqTableName, Field.Store.YES));
        colDoc.add(new StringField(ColumnDocFields.COLUMN, sqlFqn, Field.Store.YES));
        colDoc.add(new StoredField(ColumnDocFields.NULL_FRACTION, columnStats.nullFraction()));
        colDoc.add(new StoredField(ColumnDocFields.AVG_SIZE_IN_BYTES, columnStats.averageSizeInBytes()));
        colDoc.add(new StoredField(ColumnDocFields.APPROX_DISTINCT, columnStats.approxDistinct()));

        Streamer<T> streamer = columnStats.type().streamer();
        try (var out = new BytesStreamOutput()) {
            DataTypes.toStream(columnStats.type(), out);
            colDoc.add(new StoredField(ColumnDocFields.VALUE_TYPE, out.bytes().toBytesRef()));
        }
        try (var out = new BytesStreamOutput()) {
            columnStats.mostCommonValues().writeTo(streamer, out);
            BytesRef bytesRef = out.bytes().toBytesRef();
            colDoc.add(new StoredField(ColumnDocFields.MOST_COMMON_VALUES, bytesRef));
        }
        try (var out = new BytesStreamOutput()) {
            List<T> histogram = columnStats.histogram();
            out.writeVInt(histogram.size());
            for (T value : histogram) {
                streamer.writeValueTo(out, value);
            }
            BytesRef bytesRef = out.bytes().toBytesRef();
            colDoc.add(new StoredField(ColumnDocFields.HISTOGRAM, bytesRef));
        }
        return colDoc;
    }

    private static <T> T decode(Version version, Writeable.Reader<T> reader, BytesRef bytesRef) throws IOException {
        try (var in = StreamInput.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length)) {
            in.setVersion(version);
            return reader.read(in);
        }
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
}

