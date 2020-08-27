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

package org.elasticsearch.index.engine;

import static java.util.Collections.emptyList;
import static java.util.Collections.shuffle;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PEER_RECOVERY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;

public abstract class EngineTestCase extends ESTestCase {

    protected final ShardId shardId = new ShardId(new Index("index", "_na_"), 0);
    protected final AllocationId allocationId = AllocationId.newInitializing();
    protected static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    protected ThreadPool threadPool;
    protected TranslogHandler translogHandler;

    protected Store store;
    protected Store storeReplica;

    protected InternalEngine engine;
    protected InternalEngine replicaEngine;

    protected IndexSettings defaultSettings;
    protected String codecName;
    protected Path primaryTranslogDir;
    protected Path replicaTranslogDir;
    // A default primary term is used by engine instances created in this test.
    protected final PrimaryTermSupplier primaryTerm = new PrimaryTermSupplier(0L);

    protected Settings indexSettings() {
        // TODO randomize more settings
        return Settings.builder()
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h") // make sure this doesn't kick in on us
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.getKey(),
                between(10, 10 * IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.get(Settings.EMPTY)))
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean())
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(),
                randomBoolean() ? IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.get(Settings.EMPTY) : between(0, 1000))
            .build();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm.set(randomLongBetween(1, Long.MAX_VALUE));
        CodecService codecService = new CodecService(null, logger);
        String name = Codec.getDefault().getName();
        if (Arrays.asList(codecService.availableCodecs()).contains(name)) {
            // some codecs are read only so we only take the ones that we have in the service and randomly
            // selected by lucene test case.
            codecName = name;
        } else {
            codecName = "default";
        }
        defaultSettings = IndexSettingsModule.newIndexSettings("test", indexSettings());
        threadPool = new TestThreadPool(getClass().getName());
        store = createStore();
        storeReplica = createStore();
        Lucene.cleanLuceneIndex(store.directory());
        Lucene.cleanLuceneIndex(storeReplica.directory());
        primaryTranslogDir = createTempDir("translog-primary");
        translogHandler = createTranslogHandler(defaultSettings);
        engine = createEngine(store, primaryTranslogDir);
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();

        assertEquals(engine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
        replicaTranslogDir = createTempDir("translog-replica");
        replicaEngine = createEngine(storeReplica, replicaTranslogDir);
        currentIndexWriterConfig = replicaEngine.getCurrentIndexWriterConfig();

        assertEquals(replicaEngine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
    }

    public EngineConfig copy(EngineConfig config, LongSupplier globalCheckpointSupplier) {
        return new EngineConfig(
            config.getShardId(),
            config.getAllocationId(),
            config.getThreadPool(),
            config.getIndexSettings(),
            config.getStore(),
            config.getMergePolicy(),
            config.getAnalyzer(),
            new CodecService(null, logger),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            config.getTranslogConfig(),
            config.getFlushMergesAfter(),
            config.getExternalRefreshListener(),
            Collections.emptyList(),
            config.getCircuitBreakerService(),
            globalCheckpointSupplier,
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            tombstoneDocSupplier()
        );
    }

    public EngineConfig copy(EngineConfig config, Analyzer analyzer) {
        return new EngineConfig(
            config.getShardId(),
            config.getAllocationId(),
            config.getThreadPool(),
            config.getIndexSettings(),
            config.getStore(),
            config.getMergePolicy(),
            analyzer,
            new CodecService(null, logger),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            config.getTranslogConfig(),
            config.getFlushMergesAfter(),
            config.getExternalRefreshListener(),
            Collections.emptyList(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            config.getTombstoneDocSupplier());
    }

    public EngineConfig copy(EngineConfig config, MergePolicy mergePolicy) {
        return new EngineConfig(
            config.getShardId(),
            config.getAllocationId(),
            config.getThreadPool(),
            config.getIndexSettings(),
            config.getStore(),
            mergePolicy,
            config.getAnalyzer(),
            new CodecService(null, logger),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            config.getTranslogConfig(),
            config.getFlushMergesAfter(),
            config.getExternalRefreshListener(),
            Collections.emptyList(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            config.getTombstoneDocSupplier()
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            if (engine != null && engine.isClosed.get() == false) {
                engine.getTranslog().getDeletionPolicy().assertNoOpenTranslogRefs();
                assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine, createMapperService("test"));
                assertMaxSeqNoInCommitUserData(engine);
            }
            if (replicaEngine != null && replicaEngine.isClosed.get() == false) {
                replicaEngine.getTranslog().getDeletionPolicy().assertNoOpenTranslogRefs();
                assertConsistentHistoryBetweenTranslogAndLuceneIndex(replicaEngine, createMapperService("test"));
                assertMaxSeqNoInCommitUserData(replicaEngine);
            }
            assertThat(engine.config().getCircuitBreakerService().getBreaker(CircuitBreaker.ACCOUNTING).getUsed(), equalTo(0L));
            assertThat(replicaEngine.config().getCircuitBreakerService().getBreaker(CircuitBreaker.ACCOUNTING).getUsed(), equalTo(0L));
        } finally {
            IOUtils.close(replicaEngine, storeReplica, engine, store, () -> terminate(threadPool));
        }
    }


    protected static ParseContext.Document testDocumentWithTextField() {
        return testDocumentWithTextField("test");
    }

    protected static ParseContext.Document testDocumentWithTextField(String value) {
        ParseContext.Document document = testDocument();
        document.add(new TextField("value", value, Field.Store.YES));
        return document;
    }


    protected static ParseContext.Document testDocument() {
        return new ParseContext.Document();
    }

    public static ParsedDocument createParsedDoc(String id, String routing) {
        return testParsedDocument(id, routing, testDocumentWithTextField(), new BytesArray("{ \"value\" : \"test\" }"), null);
    }

    public static ParsedDocument createParsedDoc(String id, String routing, boolean recoverySource) {
        return testParsedDocument(id, routing, testDocumentWithTextField(), new BytesArray("{ \"value\" : \"test\" }"), null,
            recoverySource);
    }

    protected static ParsedDocument testParsedDocument(
        String id, String routing, ParseContext.Document document, BytesReference source, Mapping mappingUpdate) {
        return testParsedDocument(id, routing, document, source, mappingUpdate, false);
    }

    protected static ParsedDocument testParsedDocument(
        String id, String routing, ParseContext.Document document, BytesReference source, Mapping mappingUpdate,
        boolean recoverySource) {
        Field uidField = new Field("_id", Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        BytesRef ref = source.toBytesRef();
        if (recoverySource) {
            document.add(new StoredField(SourceFieldMapper.RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
            document.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_NAME, 1));
        } else {
            document.add(new StoredField(SourceFieldMapper.NAME, ref.bytes, ref.offset, ref.length));
        }
        return new ParsedDocument(versionField, seqID, id, routing, Arrays.asList(document), source, mappingUpdate);
    }

    /**
     * Creates a tombstone document that only includes uid, seq#, term and version fields.
     */
    public static EngineConfig.TombstoneDocSupplier tombstoneDocSupplier() {
        return new EngineConfig.TombstoneDocSupplier() {
            @Override
            public ParsedDocument newDeleteTombstoneDoc(String id) {
                final ParseContext.Document doc = new ParseContext.Document();
                Field uidField = new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
                doc.add(uidField);
                Field versionField = new NumericDocValuesField(VersionFieldMapper.NAME, 0);
                doc.add(versionField);
                SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
                doc.add(seqID.seqNo);
                doc.add(seqID.seqNoDocValue);
                doc.add(seqID.primaryTerm);
                seqID.tombstoneField.setLongValue(1);
                doc.add(seqID.tombstoneField);
                return new ParsedDocument(
                    versionField, seqID, id, null, Collections.singletonList(doc), new BytesArray("{}"), null);
            }

            @Override
            public ParsedDocument newNoopTombstoneDoc(String reason) {
                final ParseContext.Document doc = new ParseContext.Document();
                SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
                doc.add(seqID.seqNo);
                doc.add(seqID.seqNoDocValue);
                doc.add(seqID.primaryTerm);
                seqID.tombstoneField.setLongValue(1);
                doc.add(seqID.tombstoneField);
                Field versionField = new NumericDocValuesField(VersionFieldMapper.NAME, 0);
                doc.add(versionField);
                BytesRef byteRef = new BytesRef(reason);
                doc.add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
                return new ParsedDocument(
                    versionField, seqID, null, null, Collections.singletonList(doc), null, null);
            }
        };
    }

    protected Store createStore() throws IOException {
        return createStore(newDirectory());
    }

    protected Store createStore(final Directory directory) throws IOException {
        return createStore(INDEX_SETTINGS, directory);
    }

    protected Store createStore(final IndexSettings indexSettings, final Directory directory) throws IOException {
        return new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
    }

    protected Translog createTranslog(LongSupplier primaryTermSupplier) throws IOException {
        return createTranslog(primaryTranslogDir, primaryTermSupplier);
    }

    protected Translog createTranslog(Path translogPath, LongSupplier primaryTermSupplier) throws IOException {
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE);
        String translogUUID = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId,
            primaryTermSupplier.getAsLong());
        return new Translog(translogConfig, translogUUID, createTranslogDeletionPolicy(INDEX_SETTINGS),
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTermSupplier, seqNo -> {});
    }

    protected TranslogHandler createTranslogHandler(IndexSettings indexSettings) {
        return new TranslogHandler(xContentRegistry(), indexSettings);
    }

    protected InternalEngine createEngine(Store store, Path translogPath) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null);
    }

    protected InternalEngine createEngine(Store store, Path translogPath, LongSupplier globalCheckpointSupplier) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(
        Store store,
        Path translogPath,
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null, localCheckpointTrackerSupplier, null);
    }

    protected InternalEngine createEngine(
        Store store,
        Path translogPath,
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation) throws IOException {
        return createEngine(
            defaultSettings, store, translogPath, newMergePolicy(), null, localCheckpointTrackerSupplier, null, seqNoForOperation);
    }

    protected InternalEngine createEngine(
        IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, null);

    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                                          @Nullable IndexWriterFactory indexWriterFactory) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, indexWriterFactory, null, null);
    }

    protected InternalEngine createEngine(
        IndexSettings indexSettings,
        Store store,
        Path translogPath,
        MergePolicy mergePolicy,
        @Nullable IndexWriterFactory indexWriterFactory,
        @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        @Nullable LongSupplier globalCheckpointSupplier) throws IOException {
        return createEngine(
            indexSettings, store, translogPath, mergePolicy, indexWriterFactory, localCheckpointTrackerSupplier, null,
            globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(
        IndexSettings indexSettings,
        Store store,
        Path translogPath,
        MergePolicy mergePolicy,
        @Nullable IndexWriterFactory indexWriterFactory,
        @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        @Nullable LongSupplier globalCheckpointSupplier,
        @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation) throws IOException {
        return createEngine(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            indexWriterFactory,
            localCheckpointTrackerSupplier,
            seqNoForOperation,
            globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(
        IndexSettings indexSettings,
        Store store,
        Path translogPath,
        MergePolicy mergePolicy,
        @Nullable IndexWriterFactory indexWriterFactory,
        @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
        @Nullable LongSupplier globalCheckpointSupplier) throws IOException {
        EngineConfig config = config(indexSettings, store, translogPath, mergePolicy, null, globalCheckpointSupplier);
        return createEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation, config);
    }

    protected InternalEngine createEngine(EngineConfig config) throws IOException {
        return createEngine(null, null, null, config);
    }

    protected InternalEngine createEngine(@Nullable IndexWriterFactory indexWriterFactory,
                                          @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
                                          @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
                                          EngineConfig config) throws IOException {
        final Store store = config.getStore();
        final Directory directory = store.directory();
        if (Lucene.indexExists(directory) == false) {
            store.createEmpty();
            final String translogUuid = Translog.createEmptyTranslog(config.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            store.associateIndexWithNewTranslog(translogUuid);

        }
        InternalEngine internalEngine = createInternalEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation, config);
        internalEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        return internalEngine;
    }

    @FunctionalInterface
    public interface IndexWriterFactory {

        IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException;
    }

    /**
     * Generate a new sequence number and return it. Only works on InternalEngines
     */
    public static long generateNewSeqNo(final Engine engine) {
        assert engine instanceof InternalEngine : "expected InternalEngine, got: " + engine.getClass();
        InternalEngine internalEngine = (InternalEngine) engine;
        return internalEngine.getLocalCheckpointTracker().generateSeqNo();
    }

    public static InternalEngine createInternalEngine(
        @Nullable final IndexWriterFactory indexWriterFactory,
        @Nullable final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        @Nullable final ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
        final EngineConfig config) {
        if (localCheckpointTrackerSupplier == null) {
            return new InternalTestEngine(config) {
                @Override
                IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    return (indexWriterFactory != null) ?
                        indexWriterFactory.createWriter(directory, iwc) :
                        super.createWriter(directory, iwc);
                }

                @Override
                protected long doGenerateSeqNoForOperation(final Operation operation) {
                    return seqNoForOperation != null
                        ? seqNoForOperation.applyAsLong(this, operation)
                        : super.doGenerateSeqNoForOperation(operation);
                }
            };
        } else {
            return new InternalTestEngine(config, localCheckpointTrackerSupplier) {
                @Override
                IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    return (indexWriterFactory != null) ?
                        indexWriterFactory.createWriter(directory, iwc) :
                        super.createWriter(directory, iwc);
                }

                @Override
                protected long doGenerateSeqNoForOperation(final Operation operation) {
                    return seqNoForOperation != null
                        ? seqNoForOperation.applyAsLong(this, operation)
                        : super.doGenerateSeqNoForOperation(operation);
                }
            };
        }

    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener refreshListener) {
        return config(indexSettings, store, translogPath, mergePolicy, refreshListener, () -> SequenceNumbers.NO_OPS_PERFORMED);
    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener refreshListener, LongSupplier globalCheckpointSupplier) {
        return config(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            refreshListener,
            globalCheckpointSupplier,
            globalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY
        );
    }


    public EngineConfig config(
            final IndexSettings indexSettings,
            final Store store,
            final Path translogPath,
            final MergePolicy mergePolicy,
            final ReferenceManager.RefreshListener refreshListener,
            final LongSupplier globalCheckpointSupplier,
            final Supplier<RetentionLeases> retentionLeasesSupplier) {
        return config(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            refreshListener,
            null,
            globalCheckpointSupplier,
            retentionLeasesSupplier
        );
     }

    public EngineConfig config(IndexSettings indexSettings,
                               Store store,
                               Path translogPath,
                               MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener externalRefreshListener,
                               ReferenceManager.RefreshListener internalRefreshListener,
                               @Nullable LongSupplier maybeGlobalCheckpointSupplier) {
        return config(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            externalRefreshListener,
            internalRefreshListener,
            maybeGlobalCheckpointSupplier,
            maybeGlobalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY);
    }

    public EngineConfig config(IndexSettings indexSettings,
                               Store store,
                               Path translogPath,
                               MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener externalRefreshListener,
                               ReferenceManager.RefreshListener internalRefreshListener,
                               @Nullable LongSupplier maybeGlobalCheckpointSupplier,
                               @Nullable Supplier<RetentionLeases> maybeRetentionLeasesSupplier) {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        Engine.EventListener eventListener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, @Nullable Exception e) {
                // we don't need to notify anybody in this test
            }
        };
        final List<ReferenceManager.RefreshListener> extRefreshListenerList =
            externalRefreshListener == null ? emptyList() : Collections.singletonList(externalRefreshListener);
        final List<ReferenceManager.RefreshListener> intRefreshListenerList =
            internalRefreshListener == null ? emptyList() : Collections.singletonList(internalRefreshListener);


        final LongSupplier globalCheckpointSupplier;
        final Supplier<RetentionLeases> retentionLeasesSupplier;
        if (maybeGlobalCheckpointSupplier == null) {
            assert maybeRetentionLeasesSupplier == null;
            final ReplicationTracker replicationTracker = new ReplicationTracker(
                shardId,
                allocationId.getId(),
                indexSettings,
                randomNonNegativeLong(),
                SequenceNumbers.NO_OPS_PERFORMED,
                update -> {},
                () -> 0L,
                (leases, listener) -> {}
            );
            globalCheckpointSupplier = replicationTracker;
            retentionLeasesSupplier = replicationTracker::getRetentionLeases;
        } else {
            assert maybeRetentionLeasesSupplier != null;
            globalCheckpointSupplier = maybeGlobalCheckpointSupplier;
            retentionLeasesSupplier = maybeRetentionLeasesSupplier;
        }
        return new EngineConfig(
            shardId,
            allocationId.getId(),
            threadPool,
            indexSettings,
            store,
            mergePolicy,
            iwc.getAnalyzer(),
            new CodecService(null, logger),
            eventListener,
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            translogConfig,
            TimeValue.timeValueMinutes(5),
            extRefreshListenerList,
            intRefreshListenerList,
            new NoneCircuitBreakerService(),
            globalCheckpointSupplier,
            retentionLeasesSupplier,
            primaryTerm,
            tombstoneDocSupplier());
    }

    protected EngineConfig config(EngineConfig config,
                                  Store store,
                                  Path translogPath,
                                  EngineConfig.TombstoneDocSupplier tombstoneDocSupplier) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(config.getIndexSettings().getSettings())
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build()
        );
        TranslogConfig translogConfig = new TranslogConfig(
            shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        return new EngineConfig(
            config.getShardId(),
            config.getAllocationId(),
            config.getThreadPool(),
            indexSettings,
            store,
            config.getMergePolicy(),
            config.getAnalyzer(),
            new CodecService(null, logger),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            translogConfig,
            config.getFlushMergesAfter(),
            config.getExternalRefreshListener(),
            config.getInternalRefreshListener(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            tombstoneDocSupplier);
    }

    protected EngineConfig noOpConfig(IndexSettings indexSettings, Store store, Path translogPath) {
        return noOpConfig(indexSettings, store, translogPath, null);
    }

    protected EngineConfig noOpConfig(IndexSettings indexSettings, Store store, Path translogPath, LongSupplier globalCheckpointSupplier) {
        return config(indexSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier);
    }

    protected static final BytesReference B_1 = new BytesArray(new byte[]{1});
    protected static final BytesReference B_2 = new BytesArray(new byte[]{2});
    protected static final BytesReference B_3 = new BytesArray(new byte[]{3});
    protected static final BytesArray SOURCE = bytesArray("{}");

    protected static BytesArray bytesArray(String string) {
        return new BytesArray(string.getBytes(Charset.defaultCharset()));
    }

    public static Term newUid(String id) {
        return new Term("_id", Uid.encodeId(id));
    }

    public static Term newUid(ParsedDocument doc) {
        return newUid(doc.id());
    }

    protected Engine.Get newGet(ParsedDocument doc) {
        return new Engine.Get(doc.id(), newUid(doc));
    }

    protected Engine.Index indexForDoc(ParsedDocument doc) {
        return new Engine.Index(
            newUid(doc),
            doc,
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1,
            false,
            UNASSIGNED_SEQ_NO,
            0
        );
    }

    protected Engine.Index replicaIndexForDoc(ParsedDocument doc,
                                              long version,
                                              long seqNo,
                                              boolean isRetry) {
        return new Engine.Index(
            newUid(doc),
            doc,
            seqNo,
            primaryTerm.get(),
            version,
            null,
            Engine.Operation.Origin.REPLICA,
            System.nanoTime(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            isRetry,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    protected Engine.Delete replicaDeleteForDoc(String id, long version, long seqNo, long startTime) {
        return new Engine.Delete(
            id,
            newUid(id),
            seqNo,
            1,
            version,
            null,
            Engine.Operation.Origin.REPLICA,
            startTime,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    protected static void assertVisibleCount(InternalEngine engine, int numDocs) throws IOException {
        assertVisibleCount(engine, numDocs, true);
    }

    protected static void assertVisibleCount(InternalEngine engine, int numDocs, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.search(new MatchAllDocsQuery(), collector);
            assertThat(collector.getTotalHits(), equalTo(numDocs));
        }
    }

    public static List<Engine.Operation> generateSingleDocHistory(
        final boolean forReplica,
        final VersionType versionType,
        final boolean partialOldPrimary,
        final long primaryTerm,
        final int minOpCount,
        final int maxOpCount,
        final String docId) {
        final int numOfOps = randomIntBetween(minOpCount, maxOpCount);
        final List<Engine.Operation> ops = new ArrayList<>();
        final Term id = newUid(docId);
        final int startWithSeqNo;
        if (partialOldPrimary) {
            startWithSeqNo = randomBoolean() ? numOfOps - 1 : randomIntBetween(0, numOfOps - 1);
        } else {
            startWithSeqNo = 0;
        }
        final String valuePrefix = (forReplica ? "r_" : "p_") + docId + "_";
        final boolean incrementTermWhenIntroducingSeqNo = randomBoolean();
        for (int i = 0; i < numOfOps; i++) {
            final Engine.Operation op;
            final long version;
            switch (versionType) {
                case INTERNAL:
                    version = forReplica ? i : Versions.MATCH_ANY;
                    break;
                case EXTERNAL:
                    version = i;
                    break;
                case EXTERNAL_GTE:
                    version = randomBoolean() ? Math.max(i - 1, 0) : i;
                    break;
                case FORCE:
                    version = randomNonNegativeLong();
                    break;
                default:
                    throw new UnsupportedOperationException("unknown version type: " + versionType);
            }
            if (randomBoolean()) {
                op = new Engine.Index(
                    id,
                    testParsedDocument(docId, null, testDocumentWithTextField(valuePrefix + i), SOURCE, null),
                    forReplica && i >= startWithSeqNo ? i * 2 : UNASSIGNED_SEQ_NO,
                    forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm,
                    version,
                    forReplica ? null : versionType,
                    forReplica ? REPLICA : PRIMARY,
                    System.currentTimeMillis(), -1, false,
                    UNASSIGNED_SEQ_NO,
                    0
                );
            } else {
                op = new Engine.Delete(
                    docId,
                    id,
                    forReplica && i >= startWithSeqNo ? i * 2 : UNASSIGNED_SEQ_NO,
                    forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm,
                    version,
                    forReplica ? null : versionType,
                    forReplica ? REPLICA : PRIMARY,
                    System.currentTimeMillis(),
                    UNASSIGNED_SEQ_NO,
                    0
                );
            }
            ops.add(op);
        }
        return ops;
    }

    public List<Engine.Operation> generateHistoryOnReplica(int numOps, boolean allowGapInSeqNo, boolean allowDuplicate) throws Exception {
        long seqNo = 0;
        final int maxIdValue = randomInt(numOps * 2);
        final List<Engine.Operation> operations = new ArrayList<>(numOps);
        for (int i = 0; i < numOps; i++) {
            final String id = Integer.toString(randomInt(maxIdValue));
            final Engine.Operation.TYPE opType = randomFrom(Engine.Operation.TYPE.values());
            final long startTime = threadPool.relativeTimeInMillis();
            final int copies = allowDuplicate && rarely() ? between(2, 4) : 1;
            for (int copy = 0; copy < copies; copy++) {
                final ParsedDocument doc = createParsedDoc(id, null);
                switch (opType) {
                    case INDEX:
                        operations.add(new Engine.Index(
                            EngineTestCase.newUid(doc),
                            doc,
                            seqNo,
                            primaryTerm.get(),
                            i,
                            null,
                            randomFrom(REPLICA, PEER_RECOVERY),
                            startTime,
                            -1,
                            true,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        ));
                        break;
                    case DELETE:
                        operations.add(new Engine.Delete(
                            doc.id(),
                            EngineTestCase.newUid(doc),
                            seqNo,
                            primaryTerm.get(),
                            i,
                            null,
                            randomFrom(REPLICA, PEER_RECOVERY),
                            startTime,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        ));
                        break;
                    case NO_OP:
                        operations.add(new Engine.NoOp(
                            seqNo,
                            primaryTerm.get(),
                            randomFrom(REPLICA, PEER_RECOVERY),
                            startTime,
                            "test-" + i
                        ));
                        break;
                    default:
                        throw new IllegalStateException("Unknown operation type [" + opType + "]");
                }
            }
            seqNo++;
            if (allowGapInSeqNo && rarely()) {
                seqNo++;
            }
        }
        Randomness.shuffle(operations);
        return operations;
    }

    public static void assertOpsOnReplica(
        final List<Engine.Operation> ops,
        final InternalEngine replicaEngine,
        boolean shuffleOps,
        final Logger logger) throws IOException {
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.docs().get(0).get("value");
        } else {
            // delete
            lastFieldValue = null;
        }
        if (shuffleOps) {
            int firstOpWithSeqNo = 0;
            while (firstOpWithSeqNo < ops.size() && ops.get(firstOpWithSeqNo).seqNo() < 0) {
                firstOpWithSeqNo++;
            }
            // shuffle ops but make sure legacy ops are first
            shuffle(ops.subList(0, firstOpWithSeqNo), random());
            shuffle(ops.subList(firstOpWithSeqNo, ops.size()), random());
        }
        boolean firstOp = true;
        for (Engine.Operation op : ops) {
            logger.info("performing [{}], v [{}], seq# [{}], term [{}]",
                op.operationType().name().charAt(0), op.version(), op.seqNo(), op.primaryTerm());
            if (op instanceof Engine.Index) {
                Engine.IndexResult result = replicaEngine.index((Engine.Index) op);
                // replicas don't really care to about creation status of documents
                // this allows to ignore the case where a document was found in the live version maps in
                // a delete state and return false for the created flag in favor of code simplicity
                // as deleted or not. This check is just signal regression so a decision can be made if it's
                // intentional
                assertThat(result.isCreated(), equalTo(firstOp));
                assertThat(result.getVersion(), equalTo(op.version()));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));

            } else {
                Engine.DeleteResult result = replicaEngine.delete((Engine.Delete) op);
                // Replicas don't really care to about found status of documents
                // this allows to ignore the case where a document was found in the live version maps in
                // a delete state and return true for the found flag in favor of code simplicity
                // his check is just signal regression so a decision can be made if it's
                // intentional
                assertThat(result.isFound(), equalTo(firstOp == false));
                assertThat(result.getVersion(), equalTo(op.version()));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }
            if (randomBoolean()) {
                replicaEngine.refresh("test");
            }
            if (randomBoolean()) {
                replicaEngine.flush();
                replicaEngine.refresh("test");
            }
            firstOp = false;
        }

        assertVisibleCount(replicaEngine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    public static void concurrentlyApplyOps(List<Engine.Operation> ops, InternalEngine engine) throws InterruptedException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        CountDownLatch startGun = new CountDownLatch(thread.length);
        AtomicInteger offset = new AtomicInteger(-1);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread(() -> {
                startGun.countDown();
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                int docOffset;
                while ((docOffset = offset.incrementAndGet()) < ops.size()) {
                    try {
                        final Engine.Operation op = ops.get(docOffset);
                        if (op instanceof Engine.Index) {
                            engine.index((Engine.Index) op);
                        } else if (op instanceof Engine.Delete) {
                            engine.delete((Engine.Delete) op);
                        } else {
                            engine.noOp((Engine.NoOp) op);
                        }
                        if ((docOffset + 1) % 4 == 0) {
                            engine.refresh("test");
                        }
                        if (rarely()) {
                            engine.flush();
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            });
            thread[i].start();
        }
        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
    }

    public static void applyOperations(Engine engine, List<Engine.Operation> operations) throws IOException {
        for (Engine.Operation operation : operations) {
            applyOperation(engine, operation);
            if (randomInt(100) < 10) {
                engine.refresh("test");
            }
            if (rarely()) {
                engine.flush();
            }
        }
    }

    public static Engine.Result applyOperation(Engine engine, Engine.Operation operation) throws IOException {
        final Engine.Result result;
        switch (operation.operationType()) {
            case INDEX:
                result = engine.index((Engine.Index) operation);
                break;
            case DELETE:
                result = engine.delete((Engine.Delete) operation);
                break;
            case NO_OP:
                result = engine.noOp((Engine.NoOp) operation);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    /**
     * Gets a collection of tuples of docId, sequence number, and primary term of all live documents in the provided engine.
     */
    public static List<DocIdSeqNoAndSource> getDocIds(Engine engine, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test_get_doc_ids");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test_get_doc_ids")) {
            List<DocIdSeqNoAndSource> docs = new ArrayList<>();
            for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
                LeafReader reader = leafContext.reader();
                NumericDocValues seqNoDocValues = reader.getNumericDocValues(SeqNoFieldMapper.NAME);
                NumericDocValues primaryTermDocValues = reader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
                NumericDocValues versionDocValues = reader.getNumericDocValues(VersionFieldMapper.NAME);
                Bits liveDocs = reader.getLiveDocs();
                for (int i = 0; i < reader.maxDoc(); i++) {
                    if (liveDocs == null || liveDocs.get(i)) {
                        if (primaryTermDocValues.advanceExact(i) == false) {
                            // We have to skip non-root docs because its _id field is not stored (indexed only).
                            continue;
                        }
                        final long primaryTerm = primaryTermDocValues.longValue();
                        Document doc = reader.document(i, Sets.newHashSet(IdFieldMapper.NAME, SourceFieldMapper.NAME));
                        BytesRef binaryID = doc.getBinaryValue(IdFieldMapper.NAME);
                        String id = Uid.decodeId(Arrays.copyOfRange(binaryID.bytes, binaryID.offset, binaryID.offset + binaryID.length));
                        final BytesRef source = doc.getBinaryValue(SourceFieldMapper.NAME);
                        if (seqNoDocValues.advanceExact(i) == false) {
                            throw new AssertionError("seqNoDocValues not found for doc[" + i + "] id[" + id + "]");
                        }
                        final long seqNo = seqNoDocValues.longValue();
                        if (versionDocValues.advanceExact(i) == false) {
                            throw new AssertionError("versionDocValues not found for doc[" + i + "] id[" + id + "]");
                        }
                        final long version = versionDocValues.longValue();
                        docs.add(new DocIdSeqNoAndSource(id, source, seqNo, primaryTerm, version));
                    }
                }
            }
            docs.sort(Comparator.comparingLong(DocIdSeqNoAndSource::getSeqNo)
                .thenComparingLong(DocIdSeqNoAndSource::getPrimaryTerm)
                .thenComparing((DocIdSeqNoAndSource::getId)));
            return docs;
        }
    }

    /**
     * Reads all engine operations that have been processed by the engine from Lucene index.
     * The returned operations are sorted and de-duplicated, thus each sequence number will be have at most one operation.
     */
    public static List<Translog.Operation> readAllOperationsInLucene(Engine engine, MapperService mapper) throws IOException {
        final List<Translog.Operation> operations = new ArrayList<>();
        long maxSeqNo = Math.max(0, ((InternalEngine) engine).getLocalCheckpointTracker().getMaxSeqNo());
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", mapper, 0, maxSeqNo, false)) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                operations.add(op);
            }
        }
        return operations;
    }

    /**
     * Asserts the provided engine has a consistent document history between translog and Lucene index.
     */
    public static void assertConsistentHistoryBetweenTranslogAndLuceneIndex(Engine engine, MapperService mapper) throws IOException {
        if (mapper.types().isEmpty() || engine.config().getIndexSettings().isSoftDeleteEnabled() == false
            || (engine instanceof InternalEngine) == false) {
            return;
        }
        final long maxSeqNo = ((InternalEngine) engine).getLocalCheckpointTracker().getMaxSeqNo();
        if (maxSeqNo < 0) {
            return; // nothing to check
        }
        final Map<Long, Translog.Operation> translogOps = new HashMap<>();
        try (Translog.Snapshot snapshot = EngineTestCase.getTranslog(engine).newSnapshot()) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                translogOps.put(op.seqNo(), op);
            }
        }
        final Map<Long, Translog.Operation> luceneOps = readAllOperationsInLucene(engine, mapper).stream()
            .collect(Collectors.toMap(Translog.Operation::seqNo, Function.identity()));
        final long globalCheckpoint = EngineTestCase.getTranslog(engine).getLastSyncedGlobalCheckpoint();
        final long retainedOps = engine.config().getIndexSettings().getSoftDeleteRetentionOperations();
        final long seqNoForRecovery;
        try (Engine.IndexCommitRef safeCommit = engine.acquireSafeIndexCommit()) {
            seqNoForRecovery =
                Long.parseLong(safeCommit.getIndexCommit().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1;
        }
        final long minSeqNoToRetain = Math.min(seqNoForRecovery, globalCheckpoint + 1 - retainedOps);
        for (Translog.Operation translogOp : translogOps.values()) {
            final Translog.Operation luceneOp = luceneOps.get(translogOp.seqNo());
            if (luceneOp == null) {
                if (minSeqNoToRetain <= translogOp.seqNo() && translogOp.seqNo() <= maxSeqNo) {
                    fail("Operation not found seq# [" + translogOp.seqNo() + "], global checkpoint [" +
                         globalCheckpoint + "], " +
                         "retention policy [" + retainedOps + "], maxSeqNo [" + maxSeqNo + "], translog op [" +
                         translogOp + "]");
                } else {
                    continue;
                }
            }
            assertThat(luceneOp, notNullValue());
            assertThat(luceneOp.toString(), luceneOp.primaryTerm(), equalTo(translogOp.primaryTerm()));
            assertThat(luceneOp.opType(), equalTo(translogOp.opType()));
            if (luceneOp.opType() == Translog.Operation.Type.INDEX) {
                assertThat(luceneOp.getSource().source, equalTo(translogOp.getSource().source));
            }
        }
    }

    /**
     * Asserts that the max_seq_no stored in the commit's user_data is never smaller than seq_no of any document in the commit.
     */
    public static void assertMaxSeqNoInCommitUserData(Engine engine) throws Exception {
        List<IndexCommit> commits = DirectoryReader.listCommits(engine.store.directory());
        for (IndexCommit commit : commits) {
            try (DirectoryReader reader = DirectoryReader.open(commit)) {
                assertThat(Long.parseLong(commit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)),
                    greaterThanOrEqualTo(maxSeqNosInReader(reader)));
            }
        }
    }

    public static MapperService createMapperService(String type) throws IOException {
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .putMapping(type, "{\"properties\": {}}")
            .build();
        MapperService mapperService = MapperTestUtils.newMapperService(new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(), Settings.EMPTY, "test");
        mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
        return mapperService;
    }

    /**
     * Exposes a translog associated with the given engine for testing purpose.
     */
    public static Translog getTranslog(Engine engine) {
        assert engine instanceof InternalEngine : "only InternalEngines have translogs, got: " + engine.getClass();
        InternalEngine internalEngine = (InternalEngine) engine;
        return internalEngine.getTranslog();
    }

    public static final class PrimaryTermSupplier implements LongSupplier {
        private final AtomicLong term;

        PrimaryTermSupplier(long initialTerm) {
            this.term = new AtomicLong(initialTerm);
        }

        public long get() {
            return term.get();
        }

        public void set(long newTerm) {
            this.term.set(newTerm);
        }

        @Override
        public long getAsLong() {
            return get();
        }
    }

    static long maxSeqNosInReader(DirectoryReader reader) throws IOException {
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        for (LeafReaderContext leaf : reader.leaves()) {
            final NumericDocValues seqNoDocValues = leaf.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
            while (seqNoDocValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNoDocValues.longValue());
            }
        }
        return maxSeqNo;
    }
}
