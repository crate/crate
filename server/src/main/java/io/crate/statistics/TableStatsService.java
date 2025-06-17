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


import static org.elasticsearch.gateway.PersistedClusterStateService.NODE_ID_KEY;
import static org.elasticsearch.gateway.PersistedClusterStateService.NODE_VERSION_KEY;
import static org.elasticsearch.gateway.PersistedClusterStateService.createIndexWriter;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntPredicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
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
import org.elasticsearch.common.io.stream.StreamInput;
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
public class TableStatsService extends AbstractLifecycleComponent implements Runnable {

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
    private final String nodeId;

    private static final String TYPE = "_stats";
    private static final String STATS = "stats";
    private static final String RELATION_NAME = "relationName";
    public static final RelationName RELATION = RelationName.fromIndexName("doc.uservisits");


    @Inject
    public TableStatsService(NodeEnvironment nodeEnvironment,
                             Settings settings,
                             ThreadPool threadPool,
                             ClusterService clusterService,
                             Sessions sessions) throws IOException {
        this(nodeEnvironment.nodeDataPaths(), nodeEnvironment.nodeId(), settings, threadPool, clusterService, sessions);
    }

    @VisibleForTesting
    TableStatsService(Path[] dataPaths,
                      String nodeId,
                      Settings settings,
                      ThreadPool threadPool,
                      ClusterService clusterService,
                      Sessions sessions) throws IOException {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.sessions = sessions;
        refreshInterval = STATS_SERVICE_REFRESH_INTERVAL_SETTING.get(settings);
        scheduledRefresh = scheduleNextRefresh(refreshInterval);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(
            STATS_SERVICE_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        this.dataPath = dataPaths[0];
        this.nodeId = nodeId;
    }

    public Writer createWriter() throws IOException {
        final List<TableStatsService.TableStatsWriter> writers = new ArrayList<>();
        final List<Closeable> closeables = new ArrayList<>();
        boolean success = false;
        try {
                Path stats = dataPath.resolve(TYPE);
                final Directory directory = createDirectory(stats);
                closeables.add(directory);
                final IndexWriter indexWriter = createIndexWriter(directory, false);
                closeables.add(indexWriter);
                writers.add(new TableStatsService.TableStatsWriter(directory, indexWriter));
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(closeables);
            }
        }
        return new Writer(writers, nodeId);
    }

    @Nullable
    public TableStats load() throws IOException {
        String nodeId = null;
//        Version version = null;
        Path stats = dataPath.resolve(TYPE);
        if (Files.exists(stats)) {
            try (Directory dir = new NIOFSDirectory(stats)) {
                DirectoryReader reader;
                try {
                    reader = DirectoryReader.open(dir);
                } catch (IndexNotFoundException e) {
                    LOGGER.debug("No table stats found");
                    return null;
                }
                final Map<String, String> userData = reader.getIndexCommit().getUserData();
                assert userData.get(NODE_VERSION_KEY) != null;

                final String thisNodeId = userData.get(NODE_ID_KEY);
                assert thisNodeId != null;
                if (nodeId != null && nodeId.equals(thisNodeId) == false) {
                    //Do nothing, because the metadata does not belong to this node
                } else if (nodeId == null) {
                    nodeId = thisNodeId;
//                    version = Version.fromId(Integer.parseInt(userData.get(NODE_VERSION_KEY)));
                }
                IndexSearcher indexSearcher = new IndexSearcher(reader);
                indexSearcher.setQueryCache(null);
                Query query = new TermQuery(new Term(RELATION_NAME, RELATION.fqn()));
                long start = System.currentTimeMillis();
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
                                    BytesRef binaryValue = storedFields.document(docIdSetIterator.docID()).getBinaryValue(STATS);
                                    byte[] bytes = binaryValue.bytes;
                                    var stream = new ByteArrayInputStream(bytes);
                                    StreamInput streamInput = new InputStreamStreamInput(stream);
                                    Stats loaded = new Stats(streamInput);
                                    Map<RelationName, Stats> result = Map.of(RELATION, loaded);
                                    TableStats tableStats = new TableStats();
                                    tableStats.updateTableStats(result);
                                    long timeToLoad = System.currentTimeMillis() - start;
                                    System.out.println("Time to load table stats " + timeToLoad + " ms");
                                    return tableStats;
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }


    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        close();
    }

    @Override
    public void close() {
    }

    @Override
    protected void doClose() throws IOException {
        close();
    }


    public static class TableStatsWriter implements Closeable {

        private final Directory directory;
        private final IndexWriter indexWriter;

        TableStatsWriter(Directory directory, IndexWriter indexWriter) {
            this.directory = directory;
            this.indexWriter = indexWriter;
        }

        void updateDoc(Document tableStatsDoc) throws IOException {
            indexWriter.updateDocument(new Term(RELATION_NAME, RELATION.fqn()), tableStatsDoc);
        }

        void flush() throws IOException {
            this.indexWriter.flush();
        }

        void prepareCommit(String nodeId) throws IOException {
            final Map<String, String> commitData = new HashMap<>();
            commitData.put(NODE_VERSION_KEY, Integer.toString(Version.CURRENT.internalId));
            commitData.put(NODE_ID_KEY, nodeId);
            indexWriter.setLiveCommitData(commitData.entrySet());
            indexWriter.prepareCommit();
        }

        void commit() throws IOException {
            indexWriter.commit();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(indexWriter, directory);
        }
    }

    public static class Writer implements Closeable {

        private final List<TableStatsService.TableStatsWriter> tableStatsWriters;
        private final String nodeId;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Writer(List<TableStatsService.TableStatsWriter> tableStatsWriters, String nodeId) {
            this.tableStatsWriters = tableStatsWriters;
            this.nodeId = nodeId;
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new AlreadyClosedException("table stats writer is closed already");
            }
        }

        public boolean isOpen() {
            return closed.get() == false;
        }


        public void writeAndCommit(TableStats tableStats) throws IOException {
            ensureOpen();
            try {
                commit(tableStats);
            } finally {
                close();
            }
        }

        void commit(TableStats tableStats) throws IOException {
            ensureOpen();
            Stats stats = tableStats.getStats(RELATION);
            Document tableStatsDoc = makeDocument(stats);
            try {
                for (TableStatsService.TableStatsWriter tableStatsWriter : tableStatsWriters) {
                    tableStatsWriter.updateDoc(tableStatsDoc);
                    tableStatsWriter.prepareCommit(nodeId);
                    tableStatsWriter.flush();
                    tableStatsWriter.commit();
                }
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

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                IOUtils.close(tableStatsWriters);
            }
        }

        private Document makeDocument(Stats stats) throws IOException {
            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
            stats.writeTo(bytesStreamOutput);
            final Document document = new Document();
            document.add(new StringField(RELATION_NAME, RELATION.fqn(), Field.Store.NO));
            document.add(new StringField("stats", bytesStreamOutput.bytes().toBytesRef(), Field.Store.YES));
            return document;
        }
    }

    Directory createDirectory(Path path) throws IOException {
        return new NIOFSDirectory(path);
    }

    @Override
    public void run() {
        updateStats();
    }

    public void persist(TableStats tableStats) {
        try(
            Writer writer = createWriter()) {
            writer.writeAndCommit(tableStats);
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

