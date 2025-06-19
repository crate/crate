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
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntPredicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;


import io.crate.common.io.IOUtils;
import io.crate.metadata.RelationName;
import io.crate.session.BaseResultReceiver;
import io.crate.session.Session;
import io.crate.session.Sessions;
import io.crate.common.unit.TimeValue;
import io.crate.data.Row;

/**
 * Periodically refresh {@link TableStats} based on {@link #refreshInterval}.
 */
@Singleton
public class TableStatsService implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger(TableStatsService.class);

    public static final Setting<TimeValue> STATS_SERVICE_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "stats.service.interval", TimeValue.timeValueHours(24), Property.NodeScope, Property.Dynamic, Property.Exposed);

    public static final Setting<ByteSizeValue> STATS_SERVICE_THROTTLING_SETTING = Setting.byteSizeSetting(
        "stats.service.max_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB), Property.NodeScope, Property.Dynamic, Property.Exposed);

    static final String STMT = "ANALYZE";

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Sessions sessions;

    private Session session;

    @VisibleForTesting
    volatile TimeValue refreshInterval;

    @VisibleForTesting
    volatile Scheduler.ScheduledCancellable scheduledRefresh;

    private final Path dataPath;

    private static final String STATS_LOCATION = "_stats";
    private static final String RELATION_NAME_FIELD = "relationName";


    @Inject
    public TableStatsService(NodeEnvironment nodeEnvironment,
                             Settings settings,
                             ThreadPool threadPool,
                             ClusterService clusterService,
                             Sessions sessions) throws IOException {
        this(nodeEnvironment.nodeDataPaths()[0], settings, threadPool, clusterService, sessions);
    }

    @VisibleForTesting
    TableStatsService(Path dataPath,
                      Settings settings,
                      ThreadPool threadPool,
                      ClusterService clusterService,
                      Sessions sessions){
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.sessions = sessions;
        refreshInterval = STATS_SERVICE_REFRESH_INTERVAL_SETTING.get(settings);
        scheduledRefresh = scheduleNextRefresh(refreshInterval);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            STATS_SERVICE_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        this.dataPath = dataPath;
    }

    private Writer createWriter() throws IOException {
        Path path = dataPath.resolve(STATS_LOCATION);
        Directory directory = new NIOFSDirectory(path);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        indexWriterConfig.setRAMBufferSizeMB(1.0);
        indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        return new Writer(directory, indexWriter);
    }

    public TableStats loadTableStats() throws IOException {
        return null;
    }

    @Nullable
    public Stats loadStats(RelationName relationName) throws IOException {
        Path path = dataPath.resolve(STATS_LOCATION);
        if (Files.exists(path)) {
            try (Directory dir = new NIOFSDirectory(path)) {
                DirectoryReader reader;
                try {
                    reader = DirectoryReader.open(dir);
                } catch (IndexNotFoundException e) {
                    LOGGER.debug("No table stats found");
                    return null;
                }
                IndexSearcher indexSearcher = new IndexSearcher(reader);
                indexSearcher.setQueryCache(null);
                Query query = new TermQuery(new Term(RELATION_NAME_FIELD, relationName.fqn()));
                Weight weight = indexSearcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
                try (IndexReader indexReader = indexSearcher.getIndexReader()) {
                    for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
                        final Scorer scorer = weight.scorer(leafReaderContext);
                        if (scorer != null) {
                            final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                            final IntPredicate isLiveDoc = liveDocs == null ? i -> true : liveDocs::get;
                            final DocIdSetIterator docIdSetIterator = scorer.iterator();
                            StoredFields storedFields = leafReaderContext.reader().storedFields();
                            while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                if (isLiveDoc.test(docIdSetIterator.docID())) {
                                    BytesRef binaryValue = storedFields.document(docIdSetIterator.docID()).getBinaryValue(STATS_LOCATION);
                                    return new Stats(
                                        new InputStreamStreamInput(
                                            new ByteArrayInputStream(
                                                binaryValue.bytes
                                            )
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }
    
    private static class Writer implements Closeable {
        final Directory directory;
        final IndexWriter indexWriter;

        private Writer(Directory directory, IndexWriter indexWriter) {
            this.directory = directory;
            this.indexWriter = indexWriter;
        }

        void updateDoc(RelationName relationName, Document statsDoc) throws IOException {
            indexWriter.updateDocument(new Term(RELATION_NAME_FIELD, relationName.fqn()), statsDoc);
        }

        void flush() throws IOException {
            this.indexWriter.flush();
        }

        void commit() throws IOException {
            indexWriter.commit();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(indexWriter, directory);
        }

        void prepareCommit() throws IOException {
            indexWriter.prepareCommit();
        }


        public void writeAndCommit(RelationName relationName, Stats stats) throws IOException {
            try {
                commit(relationName, stats);
            } finally {
                close();
            }
        }

        void commit(RelationName relationName, Stats stats) throws IOException {
            Document tableStatsDoc = makeDocument(relationName, stats);
            try {
                updateDoc(relationName, tableStatsDoc);
                prepareCommit();
                flush();
                commit();
            } catch (Exception e) {
                try {
                    close();
                } catch (Exception e2) {
                    LOGGER.warn("failed on closing table stats writer", e2);
                    e.addSuppressed(e2);
                }
                throw e;
            } finally {
                close();
            }
        }

        private Document makeDocument(RelationName relationName, Stats stats) throws IOException {
            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
            stats.writeTo(bytesStreamOutput);
            Document document = new Document();
            document.add(new StringField(RELATION_NAME_FIELD, relationName.fqn(), Field.Store.YES));
            document.add(new StringField(STATS_LOCATION, bytesStreamOutput.bytes().toBytesRef(), Field.Store.YES));
            return document;
        }
    }

    @Override
    public void run() {
        updateStats();
    }

    public void persist(RelationName relationName, Stats stats) {
        try (
            Writer writer = createWriter()) {
            writer.writeAndCommit(relationName, stats);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            resultReceiver.completionFuture().whenComplete((res, err) -> {
                scheduledRefresh = scheduleNextRefresh(refreshInterval);
                if (err != null) {
                    LOGGER.error("Error running periodic " + STMT + "", err);
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
}

