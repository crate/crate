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
    private static final String STATS = "_stats";

    private static final String FIELD_TYPE = "type";

    private enum FieldType {
        TABLE,
        COLUMN;

        private static final List<FieldType> VALUES = List.of(FieldType.values());

        static FieldType of(int ordinal) {
            return VALUES.get(ordinal);
        }
    }

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
    private final Directory directory;
    private final IndexWriter writer;
    private final SearcherManager searcherManager;
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
            this.directory = new NIOFSDirectory(dataPath.resolve(STATS));
            IndexWriterConfig writerConfig = new IndexWriterConfig(new KeywordAnalyzer());
            writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            writerConfig.setMergeScheduler(new SerialMergeScheduler());
            this.writer = new IndexWriter(directory, writerConfig);
            this.searcherManager = new SearcherManager(writer, null);

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
        IOUtils.closeWhileHandlingException(searcherManager, writer, directory);
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
            writer.deleteDocuments(new Term(TableDocFields.NAME, relationName.fqn()));
            writer.commit();
            cache.invalidate(relationName);
        } catch (IOException e) {
            throw new UncheckedIOException("Can't delete TableStats from disk", e);
        }
    }

    public void clear() {
        try {
            writer.deleteAll();
            writer.commit();
            cache.invalidateAll();
        } catch (IOException e) {
            throw new UncheckedIOException("Can't delete TableStats from disk", e);
        }
    }

    @VisibleForTesting
    @Nullable
    Stats loadFromDisk(RelationName relationName) {
        HashMap<ColumnIdent, ColumnStats<?>> columnStatsMap = new HashMap<>();
        long numDocs = 0;
        long sizeInBytes = 0;
        IndexSearcher tablesSearcher = null;
        boolean match = false;
        try {
            searcherManager.maybeRefreshBlocking();
            tablesSearcher = searcherManager.acquire();
            tablesSearcher.setQueryCache(null);
            Query query = new TermQuery(new Term(TableDocFields.NAME, relationName.fqn()));
            Weight weight = tablesSearcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
            IndexReader reader = tablesSearcher.getIndexReader();
            TopDocs versionTopDoc = tablesSearcher.search(new FieldExistsQuery(MetaDocFields.VERSION), 1);
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
                    FieldType fieldType = FieldType.of(doc.getField(FIELD_TYPE).storedValue().getIntValue());
                    switch (fieldType) {
                        case COLUMN -> {
                            String columnName = doc.getField(ColumnDocFields.COLUMN).stringValue();
                            ColumnIdent column = ColumnIdent.of(columnName);
                            DataType<?> valueType = decode(
                                version,
                                DataTypes::fromStream,
                                doc.getBinaryValue(ColumnDocFields.VALUE_TYPE)
                            );
                            ColumnStats<?> columnStats = readColumnStats(version, doc, valueType);
                            columnStatsMap.put(column, columnStats);
                        }
                        case TABLE -> {
                            numDocs = doc.getField(TableDocFields.NUM_DOCS).storedValue().getLongValue();
                            sizeInBytes = doc.getField(TableDocFields.SIZE_IN_BYTES).storedValue().getLongValue();
                        }
                        default -> {
                            throw new AssertionError("Unexpected fieldType: " + fieldType);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        } finally {
            try {
                searcherManager.release(tablesSearcher);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        if (!match) {
            return null;
        }
        return new Stats(numDocs, sizeInBytes, columnStatsMap);
    }

    private static <T> ColumnStats<T> readColumnStats(Version version, Document doc, DataType<T> type) throws IOException {
        double nullFraction = doc.getField(ColumnDocFields.NULL_FRACTION).storedValue().getDoubleValue();
        double avgSizeInBytes = doc.getField(ColumnDocFields.AVG_SIZE_IN_BYTES).storedValue().getDoubleValue();
        double approxDistinct = doc.getField(ColumnDocFields.APPROX_DISTINCT).storedValue().getDoubleValue();
        Streamer<T> streamer = type.streamer();
        MostCommonValues<T> mostCommonValues = decode(
            version,
            in -> new MostCommonValues<T>(streamer, in),
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
            writer.deleteAll();
            Document metaDoc = new Document();
            metaDoc.add(new IntField(MetaDocFields.VERSION, Version.CURRENT.internalId, Field.Store.YES));
            writer.addDocument(metaDoc);
            for (var entry : relationsStats.entrySet()) {
                RelationName relationName = entry.getKey();
                Stats stats = entry.getValue();

                Document tableDoc = new Document();
                String fqTableName = relationName.fqn();
                tableDoc.add(new StringField(TableDocFields.NAME, fqTableName, Field.Store.NO));
                tableDoc.add(new IntField(FIELD_TYPE, FieldType.TABLE.ordinal(), Field.Store.YES));
                tableDoc.add(new StoredField(TableDocFields.NUM_DOCS, stats.numDocs()));
                tableDoc.add(new StoredField(TableDocFields.SIZE_IN_BYTES, stats.sizeInBytes()));
                writer.addDocument(tableDoc);

                for (var columnEntry : stats.statsByColumn().entrySet()) {
                    ColumnIdent column = columnEntry.getKey();
                    ColumnStats<?> columnStats = columnEntry.getValue();
                    writer.addDocument(createColDoc(fqTableName, column, columnStats));
                }
            }
            writer.commit();
            searcherManager.maybeRefresh();
            cache.invalidateAll(relationsStats.keySet());
        } catch (IOException e) {
            throw new UncheckedIOException("Can't write TableStats to disk", e);
        }
    }

    private static <T> Document createColDoc(String fqTableName, ColumnIdent column, ColumnStats<T> columnStats) throws IOException {
        String sqlFqn = column.sqlFqn();
        Document colDoc = new Document();
        colDoc.add(new StringField(ColumnDocFields.REL_NAME, fqTableName, Field.Store.YES));
        colDoc.add(new IntField(FIELD_TYPE, FieldType.COLUMN.ordinal(), Field.Store.YES));
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

