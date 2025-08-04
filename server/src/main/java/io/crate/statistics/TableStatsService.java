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
    private static final String STATS = "_stats";
    private static final String DATA_FIELD = "data";
    private static final String RELATION_NAME_FIELD = "relationName";

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Sessions sessions;
    private final Directory directory;
    private final IndexWriter writer;
    private final SearcherManager searcherManager;
    private final LoadingCache<RelationName, Stats> cache = Caffeine
        .newBuilder()
        .maximumWeight(10_000)
        .weigher((RelationName _, Stats s) -> s.statsByColumn().size())
        .build(this::loadFromDisk);

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

        try {
            this.directory = new NIOFSDirectory(dataPath.resolve(STATS));
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
            this.writer = new IndexWriter(directory, indexWriterConfig);
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
            writer.deleteDocuments(relTerm(relationName));
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
        try {
            searcherManager.maybeRefreshBlocking();
            IndexSearcher searcher = searcherManager.acquire();
            try {
                searcher.setQueryCache(null);
                Query query = new TermQuery(relTerm(relationName));
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
                        Document doc = storedFields.document(docIdSetIterator.docID());
                        BytesRef binaryValue = doc.getBinaryValue(DATA_FIELD);
                        ByteArrayInputStream bis = new ByteArrayInputStream(binaryValue.bytes);
                        InputStreamStreamInput in = new InputStreamStreamInput(bis);
                        Version version = Version.readVersion(in);
                        in.setVersion(version);
                        return Stats.readFrom(new InputStreamStreamInput(in));
                    }
                }
            } finally {
                searcherManager.release(searcher);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return null;
    }

    public void update(Map<RelationName, Stats> stats) {
        try {
            for (Map.Entry<RelationName, Stats> entry : stats.entrySet()) {
                RelationName relationName = entry.getKey();
                Document doc = makeDocument(relationName, entry.getValue());
                writer.updateDocument(relTerm(relationName), doc);
            }
            writer.commit();
            cache.invalidateAll(stats.keySet());
            searcherManager.maybeRefresh();
        } catch (IOException e) {
            throw new UncheckedIOException("Can't write TableStats to disk", e);
        }
    }

    private static Term relTerm(RelationName relationName) {
        return new Term(RELATION_NAME_FIELD, relationName.fqn());
    }

    private static Document makeDocument(RelationName relationName, Stats stats) throws IOException {
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        Version.writeVersion(Version.CURRENT, bytesStreamOutput);
        stats.writeTo(bytesStreamOutput);
        Document document = new Document();
        document.add(new StringField(RELATION_NAME_FIELD, relationName.fqn(), Field.Store.NO));
        document.add(new StoredField(DATA_FIELD, bytesStreamOutput.bytes().toBytesRef()));
        return document;
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

