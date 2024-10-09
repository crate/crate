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

import static io.crate.testing.Asserts.assertThat;
import static java.util.Collections.shuffle;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.LOCAL_RESET;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PEER_RECOVERY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SequenceIDFields;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import io.crate.common.collections.Tuple;
import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;
import io.crate.metadata.doc.SysColumns;

@Seed("1CFE74DE6B4C12A5:E8E04461DD2F1464")
public class InternalEngineTests extends EngineTestCase {

    static final long UNSET_AUTO_GENERATED_TIMESTAMP = -1L;

    @Test
    public void testVersionMapAfterAutoIDDocument() throws IOException {
        engine.refresh("warm_up");
        ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField("test"),
                                                new BytesArray("{}".getBytes(Charset.defaultCharset())));
        Engine.Index operation = randomBoolean() ?
            appendOnlyPrimary(doc, false, 1)
            : appendOnlyReplica(doc, false, 1, randomIntBetween(0, 5));
        engine.index(operation);
        assertThat(engine.isSafeAccessRequired()).isFalse();
        doc = testParsedDocument("1", testDocumentWithTextField("updated"),
                                 new BytesArray("{}".getBytes(Charset.defaultCharset())));
        Engine.Index update = indexForDoc(doc);
        engine.index(update);
        assertThat(engine.isSafeAccessRequired()).isTrue();
        assertThat(engine.getVersionMap().values()).hasSize(1);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs()).isEqualTo(0);
        }

        try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertThat(searcher.getIndexReader().numDocs()).isEqualTo(1);
            TopDocs search = searcher.search(new MatchAllDocsQuery(), 1);
            org.apache.lucene.document.Document luceneDoc = searcher.doc(search.scoreDocs[0].doc);
            assertThat(luceneDoc.get("value")).isEqualTo("test");
        }

        // now lets make this document visible
        engine.refresh("test");
        if (randomBoolean()) { // random empty refresh
            engine.refresh("test");
        }
        assertThat(engine.isSafeAccessRequired()).as("safe access should be required we carried it over").isTrue();
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs()).isEqualTo(1);
            TopDocs search = searcher.search(new MatchAllDocsQuery(), 1);
            org.apache.lucene.document.Document luceneDoc = searcher.doc(search.scoreDocs[0].doc);
            assertThat(luceneDoc.get("value")).isEqualTo("updated");
        }

        doc = testParsedDocument("2", testDocumentWithTextField("test"),
                                 new BytesArray("{}".getBytes(Charset.defaultCharset())));
        operation = randomBoolean() ?
            appendOnlyPrimary(doc, false, 1)
            : appendOnlyReplica(doc, false, 1, generateNewSeqNo(engine));
        engine.index(operation);
        assertThat(engine.isSafeAccessRequired()).as("safe access should be required").isTrue();
        assertThat(engine.getVersionMap().values()).hasSize(1); // now we add this to the map
        engine.refresh("test");
        if (randomBoolean()) { // randomly refresh here again
            engine.refresh("test");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs()).isEqualTo(2);
        }
        if (operation.origin() == PRIMARY) {
            assertThat(engine.isSafeAccessRequired()).as("safe access should NOT be required last indexing round was only append only").isFalse();
        }
        engine.delete(new Engine.Delete(
            operation.id(),
            operation.uid(),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            UNASSIGNED_SEQ_NO,
            0
        ));
        assertThat(engine.isSafeAccessRequired()).as("safe access should be required").isTrue();
        engine.refresh("test");
        assertThat(engine.isSafeAccessRequired()).as("safe access should be required").isTrue();
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs()).isEqualTo(1);
        }
    }

    @Test
    public void testSegmentsWithoutSoftDeletes() throws Exception {
        Settings settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build());
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null))) {
            List<Segment> segments = engine.segments(false);
            assertThat(segments.isEmpty()).isTrue();

            // create two docs and refresh
            ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), B_1);
            Engine.Index first = indexForDoc(doc);
            Engine.IndexResult firstResult = engine.index(first);
            ParsedDocument doc2 = testParsedDocument("2", testDocumentWithTextField(), B_2);
            Engine.Index second = indexForDoc(doc2);
            Engine.IndexResult secondResult = engine.index(second);
            assertThat(secondResult.getTranslogLocation()).isGreaterThan(firstResult.getTranslogLocation());
            engine.refresh("test");

            segments = engine.segments(false);
            assertThat(segments).hasSize(1);
            assertThat(segments.get(0).isCommitted()).isFalse();
            assertThat(segments.get(0).isSearch()).isTrue();
            assertThat(segments.get(0).getNumDocs()).isEqualTo(2);
            assertThat(segments.get(0).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(0).isCompound()).isTrue();
            assertThat(segments.get(0).getAttributes()).containsKey(Lucene90StoredFieldsFormat.MODE_KEY);

            engine.flush();

            segments = engine.segments(false);
            assertThat(segments).hasSize(1);
            assertThat(segments.get(0).isCommitted()).isTrue();
            assertThat(segments.get(0).isSearch()).isTrue();
            assertThat(segments.get(0).getNumDocs()).isEqualTo(2);
            assertThat(segments.get(0).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(0).isCompound()).isTrue();

            ParsedDocument doc3 = testParsedDocument("3", testDocumentWithTextField(), B_3);
            engine.index(indexForDoc(doc3));
            engine.refresh("test");

            segments = engine.segments(false);
            assertThat(segments).hasSize(2);
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration()).isTrue();
            assertThat(segments.get(0).isCommitted()).isTrue();
            assertThat(segments.get(0).isSearch()).isTrue();
            assertThat(segments.get(0).getNumDocs()).isEqualTo(2);
            assertThat(segments.get(0).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(0).isCompound()).isTrue();


            assertThat(segments.get(1).isCommitted()).isFalse();
            assertThat(segments.get(1).isSearch()).isTrue();
            assertThat(segments.get(1).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(1).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(1).isCompound()).isTrue();


            engine.delete(new Engine.Delete(
                "1",
                newUid(doc),
                UNASSIGNED_SEQ_NO,
                primaryTerm.get(),
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                UNASSIGNED_SEQ_NO,
                0
            ));
            engine.refresh("test");

            segments = engine.segments(false);
            assertThat(segments).hasSize(2);
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration()).isTrue();
            assertThat(segments.get(0).isCommitted()).isTrue();
            assertThat(segments.get(0).isSearch()).isTrue();
            assertThat(segments.get(0).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(0).getDeletedDocs()).isEqualTo(1);
            assertThat(segments.get(0).isCompound()).isTrue();

            assertThat(segments.get(1).isCommitted()).isFalse();
            assertThat(segments.get(1).isSearch()).isTrue();
            assertThat(segments.get(1).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(1).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(1).isCompound()).isTrue();

            engine.onSettingsChanged(indexSettings.getTranslogRetentionAge(), indexSettings.getTranslogRetentionSize(),
                                     indexSettings.getSoftDeleteRetentionOperations());
            ParsedDocument doc4 = testParsedDocument("4", testDocumentWithTextField(), B_3);
            engine.index(indexForDoc(doc4));
            engine.refresh("test");

            segments = engine.segments(false);
            assertThat(segments).hasSize(3);
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration()).isTrue();
            assertThat(segments.get(0).isCommitted()).isTrue();
            assertThat(segments.get(0).isSearch()).isTrue();
            assertThat(segments.get(0).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(0).getDeletedDocs()).isEqualTo(1);
            assertThat(segments.get(0).isCompound()).isTrue();

            assertThat(segments.get(1).isCommitted()).isFalse();
            assertThat(segments.get(1).isSearch()).isTrue();
            assertThat(segments.get(1).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(1).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(1).isCompound()).isTrue();

            assertThat(segments.get(2).isCommitted()).isFalse();
            assertThat(segments.get(2).isSearch()).isTrue();
            assertThat(segments.get(2).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(2).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(2).isCompound()).isTrue();

            // internal refresh - lets make sure we see those segments in the stats
            ParsedDocument doc5 = testParsedDocument("5", testDocumentWithTextField(), B_3);
            engine.index(indexForDoc(doc5));
            engine.refresh("test", Engine.SearcherScope.INTERNAL, true);

            segments = engine.segments(false);
            assertThat(segments).hasSize(4);
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration()).isTrue();
            assertThat(segments.get(0).isCommitted()).isTrue();
            assertThat(segments.get(0).isSearch()).isTrue();
            assertThat(segments.get(0).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(0).getDeletedDocs()).isEqualTo(1);
            assertThat(segments.get(0).isCompound()).isTrue();

            assertThat(segments.get(1).isCommitted()).isFalse();
            assertThat(segments.get(1).isSearch()).isTrue();
            assertThat(segments.get(1).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(1).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(1).isCompound()).isTrue();

            assertThat(segments.get(2).isCommitted()).isFalse();
            assertThat(segments.get(2).isSearch()).isTrue();
            assertThat(segments.get(2).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(2).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(2).isCompound()).isTrue();

            assertThat(segments.get(3).isCommitted()).isFalse();
            assertThat(segments.get(3).isSearch()).isFalse();
            assertThat(segments.get(3).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(3).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(3).isCompound()).isTrue();

            // now refresh the external searcher and make sure it has the new segment
            engine.refresh("test");
            segments = engine.segments(false);
            assertThat(segments).hasSize(4);
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration()).isTrue();
            assertThat(segments.get(0).isCommitted()).isTrue();
            assertThat(segments.get(0).isSearch()).isTrue();
            assertThat(segments.get(0).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(0).getDeletedDocs()).isEqualTo(1);
            assertThat(segments.get(0).isCompound()).isTrue();

            assertThat(segments.get(1).isCommitted()).isFalse();
            assertThat(segments.get(1).isSearch()).isTrue();
            assertThat(segments.get(1).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(1).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(1).isCompound()).isTrue();

            assertThat(segments.get(2).isCommitted()).isFalse();
            assertThat(segments.get(2).isSearch()).isTrue();
            assertThat(segments.get(2).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(2).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(2).isCompound()).isTrue();

            assertThat(segments.get(3).isCommitted()).isFalse();
            assertThat(segments.get(3).isSearch()).isTrue();
            assertThat(segments.get(3).getNumDocs()).isEqualTo(1);
            assertThat(segments.get(3).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(3).isCompound()).isTrue();
        }
    }

    @Test
    public void testVerboseSegments() throws Exception {
        try (Store store = createStore();
             Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            List<Segment> segments = engine.segments(true);
            assertThat(segments.isEmpty()).isTrue();

            ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), B_1);
            engine.index(indexForDoc(doc));
            engine.refresh("test");

            segments = engine.segments(true);
            assertThat(segments).hasSize(1);

            ParsedDocument doc2 = testParsedDocument("2", testDocumentWithTextField(), B_2);
            engine.index(indexForDoc(doc2));
            engine.refresh("test");
            ParsedDocument doc3 = testParsedDocument("3", testDocumentWithTextField(), B_3);
            engine.index(indexForDoc(doc3));
            engine.refresh("test");

            segments = engine.segments(true);
            assertThat(segments).hasSize(3);
        }
    }

    @Test
    public void testSegmentsWithMergeFlag() throws Exception {
        try (Store store = createStore();
             Engine engine = createEngine(defaultSettings, store, createTempDir(), new TieredMergePolicy())) {
            ParsedDocument doc = testParsedDocument("1", testDocument(), B_1);
            Engine.Index index = indexForDoc(doc);
            engine.index(index);
            engine.flush();
            assertThat(engine.segments(false).size()).isEqualTo(1);
            index = indexForDoc(testParsedDocument("2", testDocument(), B_1));
            engine.index(index);
            engine.flush();
            List<Segment> segments = engine.segments(false);
            assertThat(segments).hasSize(2);
            for (Segment segment : segments) {
                assertThat(segment.getMergeId()).isNull();
            }
            index = indexForDoc(testParsedDocument("3", testDocument(), B_1));
            engine.index(index);
            engine.flush();
            segments = engine.segments(false);
            assertThat(segments).hasSize(3);
            for (Segment segment : segments) {
                assertThat(segment.getMergeId()).isNull();
            }

            index = indexForDoc(doc);
            engine.index(index);
            engine.flush();
            final long gen1 = store.readLastCommittedSegmentsInfo().getGeneration();
            // now, optimize and wait for merges, see that we have no merge flag
            engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());

            for (Segment segment : engine.segments(false)) {
                assertThat(segment.getMergeId()).isNull();
            }
            // we could have multiple underlying merges, so the generation may increase more than once
            assertThat(store.readLastCommittedSegmentsInfo().getGeneration() > gen1).isTrue();

            final boolean flush = randomBoolean();
            final long gen2 = store.readLastCommittedSegmentsInfo().getGeneration();
            engine.forceMerge(flush, 1, false, UUIDs.randomBase64UUID());

            for (Segment segment : engine.segments(false)) {
                assertThat(segment.getMergeId()).isNull();
            }

            if (flush) {
                // we should have had just 1 merge, so last generation should be exact
                assertThat(store.readLastCommittedSegmentsInfo().getLastGeneration()).isEqualTo(gen2);
            }
        }
    }

    @Test
    public void testSegmentsWithSoftDeletes() throws Exception {
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null,
                                                         null, globalCheckpoint::get))) {
            assertThat(engine.segments(false)).isEmpty();
            int numDocsFirstSegment = randomIntBetween(5, 50);
            Set<String> liveDocsFirstSegment = new HashSet<>();
            for (int i = 0; i < numDocsFirstSegment; i++) {
                String id = Integer.toString(i);
                ParsedDocument doc = testParsedDocument(id, testDocument(), B_1);
                engine.index(indexForDoc(doc));
                liveDocsFirstSegment.add(id);
            }
            engine.refresh("test");
            List<Segment> segments = engine.segments(randomBoolean());
            assertThat(segments).hasSize(1);
            assertThat(segments.get(0).getNumDocs()).isEqualTo(liveDocsFirstSegment.size());
            assertThat(segments.get(0).getDeletedDocs()).isEqualTo(0);
            assertThat(segments.get(0).committed).isFalse();
            int deletes = 0;
            int updates = 0;
            int appends = 0;
            int iterations = scaledRandomIntBetween(1, 50);
            for (int i = 0; i < iterations && liveDocsFirstSegment.isEmpty() == false; i++) {
                String idToUpdate = randomFrom(liveDocsFirstSegment);
                liveDocsFirstSegment.remove(idToUpdate);
                ParsedDocument doc = testParsedDocument(idToUpdate, testDocument(), B_1);
                if (randomBoolean()) {
                    engine.delete(new Engine.Delete(doc.id(), newUid(doc), primaryTerm.get()));
                    deletes++;
                } else {
                    engine.index(indexForDoc(doc));
                    updates++;
                }
                if (randomBoolean()) {
                    engine.index(indexForDoc(testParsedDocument(UUIDs.randomBase64UUID(), testDocument(), B_1)));
                    appends++;
                }
            }
            boolean committed = randomBoolean();
            if (committed) {
                engine.flush();
            }
            engine.refresh("test");
            segments = engine.segments(randomBoolean());
            assertThat(segments).hasSize(2);
            assertThat(segments.get(0).getNumDocs()).isEqualTo(liveDocsFirstSegment.size());
            assertThat(segments.get(0).getDeletedDocs()).isEqualTo(updates + deletes);
            assertThat(segments.get(0).committed).isEqualTo(committed);

            assertThat(segments.get(1).getNumDocs()).isEqualTo(updates + appends);
            assertThat(segments.get(1).getDeletedDocs()).isEqualTo(deletes); // delete tombstones
            assertThat(segments.get(1).committed).isEqualTo(committed);
        }
    }


    @Test
    public void testCommitStats() throws IOException {
        final AtomicLong maxSeqNo = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final AtomicLong localCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final AtomicLong globalCheckpoint = new AtomicLong(UNASSIGNED_SEQ_NO);
        try (
            Store store = createStore();
            InternalEngine engine = createEngine(store, createTempDir(), (maxSeq, localCP) -> new LocalCheckpointTracker(
                                                     maxSeq,
                                                     localCP) {
                                                     @Override
                                                     public long getMaxSeqNo() {
                                                         return maxSeqNo.get();
                                                     }

                                                     @Override
                                                     public long getProcessedCheckpoint() {
                                                         return localCheckpoint.get();
                                                     }
                    }
            )) {
            CommitStats stats1 = engine.commitStats();
            assertThat(stats1.getGeneration()).isGreaterThan(0L);
            assertThat(stats1.getId()).isNotNull();
            assertThat(stats1.getUserData()).containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY);
            assertThat(
                Long.parseLong(stats1.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY))).isEqualTo(SequenceNumbers.NO_OPS_PERFORMED);

            assertThat(stats1.getUserData()).containsKey(SequenceNumbers.MAX_SEQ_NO);
            assertThat(
                Long.parseLong(stats1.getUserData().get(SequenceNumbers.MAX_SEQ_NO))).isEqualTo(SequenceNumbers.NO_OPS_PERFORMED);

            maxSeqNo.set(rarely() ? SequenceNumbers.NO_OPS_PERFORMED : randomIntBetween(0, 1024));
            localCheckpoint.set(
                rarely() || maxSeqNo.get() == SequenceNumbers.NO_OPS_PERFORMED ?
                    SequenceNumbers.NO_OPS_PERFORMED : randomIntBetween(0, 1024));
            globalCheckpoint.set(rarely() || localCheckpoint.get() == SequenceNumbers.NO_OPS_PERFORMED ?
                                     UNASSIGNED_SEQ_NO : randomIntBetween(0, (int) localCheckpoint.get()));

            final Engine.CommitId commitId = engine.flush(true, true);

            CommitStats stats2 = engine.commitStats();
            assertThat(stats2.getRawCommitId()).isEqualTo(commitId);
            assertThat(stats2.getGeneration()).isGreaterThan(stats1.getGeneration());
            assertThat(stats2.getId()).isNotNull();
            assertThat(stats2.getId()).isNotEqualTo(stats1.getId());
            assertThat(stats2.getUserData()).containsKey(Translog.TRANSLOG_UUID_KEY);
            assertThat(stats2.getUserData().get(Translog.TRANSLOG_UUID_KEY)).isEqualTo(stats1.getUserData().get(Translog.TRANSLOG_UUID_KEY));
            assertThat(Long.parseLong(stats2.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY))).isEqualTo(localCheckpoint.get());
            assertThat(stats2.getUserData()).containsKey(SequenceNumbers.MAX_SEQ_NO);
            assertThat(Long.parseLong(stats2.getUserData().get(SequenceNumbers.MAX_SEQ_NO))).isEqualTo(maxSeqNo.get());
        }
    }

    @Test
    public void testFlushIsDisabledDuringTranslogRecovery() throws IOException {
        engine.ensureCanFlush(); // recovered already
        ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), SOURCE);
        engine.index(indexForDoc(doc));
        engine.close();

        engine = new InternalEngine(engine.config());
        assertThatThrownBy(engine::ensureCanFlush)
            .isExactlyInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> engine.flush(true, true))
            .isExactlyInstanceOf(IllegalStateException.class);
        if (randomBoolean()) {
            engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        } else {
            engine.skipTranslogRecovery();
        }
        engine.ensureCanFlush(); // ready

        doc = testParsedDocument("2", testDocumentWithTextField(), SOURCE);
        engine.index(indexForDoc(doc));
        engine.flush();
    }

    @Test
    public void testTranslogMultipleOperationsSameDocument() throws IOException {
        final int ops = randomIntBetween(1, 32);
        Engine initialEngine;
        final List<Engine.Operation> operations = new ArrayList<>();
        try {
            initialEngine = engine;
            for (int i = 0; i < ops; i++) {
                final ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), SOURCE);
                if (randomBoolean()) {
                        final Engine.Index operation = new Engine.Index(
                            newUid(doc),
                            doc,
                            UNASSIGNED_SEQ_NO,
                            0,
                            i,
                            VersionType.EXTERNAL,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            -1,
                            false,
                            UNASSIGNED_SEQ_NO,
                            0
                        );
                    operations.add(operation);
                    initialEngine.index(operation);
                } else {
                    final Engine.Delete operation = new Engine.Delete(
                        "1",
                        newUid(doc),
                        UNASSIGNED_SEQ_NO,
                        0,
                        i,
                        VersionType.EXTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(),
                        UNASSIGNED_SEQ_NO,
                        0
                    );
                    operations.add(operation);
                    initialEngine.delete(operation);
                }
            }
        } finally {
            IOUtils.close(engine);
        }
        try (Engine recoveringEngine = new InternalEngine(engine.config())) {
            recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
            recoveringEngine.refresh("test");
            try (Engine.Searcher searcher = recoveringEngine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(new MatchAllDocsQuery(), collector);
                assertThat(collector.getTotalHits()).isEqualTo(operations.get(operations.size() - 1) instanceof Engine.Delete ? 0 : 1);
            }
        }
    }

    @Test
    public void testTranslogRecoveryDoesNotReplayIntoTranslog() throws IOException {
        final int docs = randomIntBetween(1, 32);
        Engine initialEngine = null;
        try {
            initialEngine = engine;
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, testDocumentWithTextField(), SOURCE);
                initialEngine.index(indexForDoc(doc));
            }
        } finally {
            IOUtils.close(initialEngine);
        }

        Engine recoveringEngine = null;
        try {
            final AtomicBoolean committed = new AtomicBoolean();
            recoveringEngine = new InternalEngine(initialEngine.config()) {

                @Override
                protected void commitIndexWriter(IndexWriter writer, Translog translog, String syncId) throws IOException {
                    committed.set(true);
                    super.commitIndexWriter(writer, translog, syncId);
                }
            };
            recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
            assertThat(committed.get()).isTrue();
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    @Test
    public void testTranslogRecoveryWithMultipleGenerations() throws IOException {
        final int docs = randomIntBetween(1, 4096);
        final List<Long> seqNos = LongStream.range(0, docs).boxed().collect(Collectors.toList());
        Randomness.shuffle(seqNos);
        Engine initialEngine = null;
        Engine recoveringEngine = null;
        Store store = createStore();
        final AtomicInteger counter = new AtomicInteger();
        try {
            initialEngine = createEngine(
                store,
                createTempDir(),
                LocalCheckpointTracker::new,
                (engine, operation) -> seqNos.get(counter.getAndIncrement()));
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, testDocumentWithTextField(), SOURCE);
                initialEngine.index(indexForDoc(doc));
                if (rarely()) {
                    getTranslog(initialEngine).rollGeneration();
                } else if (rarely()) {
                    initialEngine.flush();
                }
            }
            initialEngine.close();
            recoveringEngine = new InternalEngine(initialEngine.config());
            recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
            recoveringEngine.refresh("test");
            try (Engine.Searcher searcher = recoveringEngine.acquireSearcher("test")) {
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), docs);
                assertThat(topDocs.totalHits.value).isEqualTo(docs);
            }
        } finally {
            IOUtils.close(initialEngine, recoveringEngine, store);
        }
    }

    @Test
    public void testRecoveryFromTranslogUpToSeqNo() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(),
                                         null, null, globalCheckpoint::get);
            final long maxSeqNo;
            try (InternalEngine engine = createEngine(config)) {
                final int docs = randomIntBetween(1, 100);
                for (int i = 0; i < docs; i++) {
                    final String id = Integer.toString(i);
                    final ParsedDocument doc = testParsedDocument(id, testDocumentWithTextField(),
                                                                  SOURCE);
                    engine.index(indexForDoc(doc));
                    if (rarely()) {
                        engine.rollTranslogGeneration();
                    } else if (rarely()) {
                        engine.flush(randomBoolean(), true);
                    }
                }
                maxSeqNo = engine.getLocalCheckpointTracker().getMaxSeqNo();
                globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getProcessedLocalCheckpoint()));
                engine.syncTranslog();
            }
            try (InternalEngine engine = new InternalEngine(config)) {
                engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                assertThat(engine.getProcessedLocalCheckpoint()).isEqualTo(maxSeqNo);
                assertThat(engine.getLocalCheckpointTracker().getMaxSeqNo()).isEqualTo(maxSeqNo);
            }
            try (InternalEngine engine = new InternalEngine(config)) {
                long upToSeqNo = randomLongBetween(globalCheckpoint.get(), maxSeqNo);
                engine.recoverFromTranslog(translogHandler, upToSeqNo);
                assertThat(engine.getProcessedLocalCheckpoint()).isEqualTo(upToSeqNo);
                assertThat(engine.getLocalCheckpointTracker().getMaxSeqNo()).isEqualTo(upToSeqNo);
            }
        }
    }

    @Test
    public void testConcurrentGetAndFlush() throws Exception {
        ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), B_1);
        engine.index(indexForDoc(doc));

        final AtomicReference<Engine.GetResult> latestGetResult = new AtomicReference<>();
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        latestGetResult.set(engine.get(newGet(doc), searcherFactory));
        final AtomicBoolean flushFinished = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        Thread getThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            while (flushFinished.get() == false) {
                Engine.GetResult previousGetResult = latestGetResult.get();
                if (previousGetResult != null) {
                    previousGetResult.close();
                }
                latestGetResult.set(engine.get(newGet(doc), searcherFactory));
                if (latestGetResult.get().docIdAndVersion() == null) {
                    break;
                }
            }
        });
        getThread.start();
        barrier.await();
        engine.flush();
        flushFinished.set(true);
        getThread.join();
        assertThat(latestGetResult.get().docIdAndVersion()).isNotNull();
        latestGetResult.get().close();
    }

    @Test
    public void testSimpleOperations() throws Exception {
        engine.refresh("warm_up");
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(0);
        searchResult.close();

        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;

        // create a document
        Document document = testDocumentWithTextField();
        document.add(new Field(SysColumns.Source.NAME, BytesReference.toBytes(B_1), SysColumns.Source.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", document, B_1);
        engine.index(indexForDoc(doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(0);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 0);
        searchResult.close();

        // we can get it in realtime
        try (Engine.GetResult getResult = engine.get(newGet(doc), searcherFactory)) {
            assertThat(getResult.docIdAndVersion()).isNotNull();
        }


        // refresh and it should be there
        engine.refresh("test");

        // now its there...
        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 1);
        searchResult.close();

        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        document.add(new Field(SysColumns.Source.NAME, BytesReference.toBytes(B_2), SysColumns.Source.FIELD_TYPE));
        doc = testParsedDocument("1", document, B_2);
        engine.index(indexForDoc(doc));

        // its not updated yet...
        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test1")), 0);
        searchResult.close();

        // but, we can still get it (in realtime)
        try (Engine.GetResult getResult = engine.get(newGet(doc), searcherFactory)) {
            assertThat(getResult.docIdAndVersion()).isNotNull();
        }

        // refresh and it should be updated
        engine.refresh("test");

        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 0);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test1")), 1);
        searchResult.close();

        // now delete
        engine.delete(new Engine.Delete(
            "1",
            newUid(doc),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            UNASSIGNED_SEQ_NO,
            0
        ));

        // its not deleted yet
        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 0);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test1")), 1);
        searchResult.close();

        // but, get should not see it (in realtime)
        try (Engine.GetResult getResult = engine.get(newGet(doc), searcherFactory)) {
            assertThat(getResult.docIdAndVersion()).isNull();
        }

        // refresh and it should be deleted
        engine.refresh("test");

        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(0);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 0);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test1")), 0);
        searchResult.close();

        // add it back
        document = testDocumentWithTextField();
        document.add(new Field(SysColumns.Source.NAME, BytesReference.toBytes(B_1), SysColumns.Source.FIELD_TYPE));
        doc = testParsedDocument("1", document, B_1);
        engine.index(new Engine.Index(
            newUid(doc), doc, UNASSIGNED_SEQ_NO, primaryTerm.get(),
            Versions.MATCH_DELETED, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(0);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 0);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test1")), 0);
        searchResult.close();

        // refresh and it should be there
        engine.refresh("test");

        // now its there...
        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test1")), 0);
        searchResult.close();

        // now flush
        engine.flush();

        // and, verify get (in real time)
        try (Engine.GetResult getResult = engine.get(newGet(doc), searcherFactory)) {
            assertThat(getResult.docIdAndVersion()).isNotNull();
        }

        // make sure we can still work with the engine
        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        doc = testParsedDocument("1", document, B_1);
        engine.index(indexForDoc(doc));

        // its not updated yet...
        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test1")), 0);
        searchResult.close();

        // refresh and it should be updated
        engine.refresh("test");

        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 0);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test1")), 1);
        searchResult.close();
    }

    public void testSearchResultRelease() throws Exception {
        engine.refresh("warm_up");
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(0);
        searchResult.close();

        // create a document
        ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), B_1);
        engine.index(indexForDoc(doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(0);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 0);
        searchResult.close();

        // refresh and it should be there
        engine.refresh("test");

        // now its there...
        searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 1);
        // don't release the search result yet...

        // delete, refresh and do a new search, it should not be there
        engine.delete(new Engine.Delete(
            "1",
            newUid(doc),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            UNASSIGNED_SEQ_NO,
            0
        ));
        engine.refresh("test");
        Engine.Searcher updateSearchResult = engine.acquireSearcher("test");
        assertThat(updateSearchResult).hasTotalHits(0);
        updateSearchResult.close();

        // the non release search result should not see the deleted yet...
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(new TermQuery(new Term("value", "test")), 1);
        searchResult.close();
    }

    @Test
    public void testCommitAdvancesMinTranslogForRecovery() throws IOException {
        IOUtils.close(engine, store);
        final Path translogPath = createTempDir();
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final LongSupplier globalCheckpointSupplier = () -> globalCheckpoint.get();
        engine = createEngine(config(defaultSettings, store, translogPath, newMergePolicy(), null, null,
            globalCheckpointSupplier));
        engine.onSettingsChanged(TimeValue.MINUS_ONE, ByteSizeValue.ZERO, randomNonNegativeLong());
        ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), B_1);
        engine.index(indexForDoc(doc));
        boolean inSync = randomBoolean();
        if (inSync) {
            engine.syncTranslog(); // to advance persisted local checkpoint
            globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
        }

        engine.flush();
        assertThat(engine.getTranslog().currentFileGeneration()).isEqualTo(3L);
        assertThat(engine.getTranslog().getMinFileGeneration()).isEqualTo(inSync ? 3L : 2L);

        engine.flush();
        assertThat(engine.getTranslog().currentFileGeneration()).isEqualTo(3L);
        assertThat(engine.getTranslog().getMinFileGeneration()).isEqualTo(inSync ? 3L : 2L);

        engine.flush(true, true);
        assertThat(engine.getTranslog().currentFileGeneration()).isEqualTo(3L);
        assertThat(engine.getTranslog().getMinFileGeneration()).isEqualTo(inSync ? 3L : 2L);

        globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
        engine.flush(true, true);
        assertThat(engine.getTranslog().currentFileGeneration()).isEqualTo(3L);
        assertThat(engine.getTranslog().getMinFileGeneration()).isEqualTo(3L);
    }

    @Test
    public void testSyncTranslogConcurrently() throws Exception {
        IOUtils.close(engine, store);
        final Path translogPath = createTempDir();
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        engine = createEngine(config(defaultSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpoint::get));
        List<Engine.Operation> ops = generateHistoryOnReplica(between(1, 50), false, randomBoolean());
        applyOperations(engine, ops);
        engine.flush(true, true);
        final CheckedRunnable<IOException> checker = () -> {
            assertThat(engine.getTranslogStats().getUncommittedOperations()).isEqualTo(0);
            assertThat(engine.getLastSyncedGlobalCheckpoint()).isEqualTo(globalCheckpoint.get());
            try (Engine.IndexCommitRef safeCommit = engine.acquireSafeIndexCommit()) {
                SequenceNumbers.CommitInfo commitInfo =
                    SequenceNumbers.loadSeqNoInfoFromLuceneCommit(safeCommit.getIndexCommit().getUserData().entrySet());
                assertThat(commitInfo.localCheckpoint).isEqualTo(engine.getProcessedLocalCheckpoint());
            }
        };
        final Thread[] threads = new Thread[randomIntBetween(2, 4)];
        final Phaser phaser = new Phaser(threads.length);
        globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                try {
                    engine.syncTranslog();
                    checker.run();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        checker.run();
    }

    @Test
    public void testSyncedFlush() throws IOException {
        try (Store store = createStore();
             Engine engine = createEngine(defaultSettings, store, createTempDir(), new LogByteSizeMergePolicy(), null)) {
            final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
            ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), B_1);
            engine.index(indexForDoc(doc));
            Engine.CommitId commitID = engine.flush();
            assertThat(commitID).isEqualTo(new Engine.CommitId(store.readLastCommittedSegmentsInfo().getId()));
            byte[] wrongBytes = Base64.getDecoder().decode(commitID.toString());
            wrongBytes[0] = (byte) ~wrongBytes[0];
            Engine.CommitId wrongId = new Engine.CommitId(wrongBytes);
            assertThat(Engine.SyncedFlushResult.COMMIT_MISMATCH).as("should fail to sync flush with wrong id (but no docs)").isEqualTo(engine.syncFlush(syncId + "1", wrongId));
            engine.index(indexForDoc(doc));
            assertThat(Engine.SyncedFlushResult.PENDING_OPERATIONS).as("should fail to sync flush with right id but pending doc").isEqualTo(engine.syncFlush(syncId + "2", commitID));
            commitID = engine.flush();
            assertThat(Engine.SyncedFlushResult.SUCCESS).as("should succeed to flush commit with right id and no pending doc").isEqualTo(engine.syncFlush(syncId, commitID));
            assertThat(syncId).isEqualTo(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID));
            assertThat(syncId).isEqualTo(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }

    @Test
    public void testRenewSyncFlush() throws Exception {
        final int iters = randomIntBetween(2, 5); // run this a couple of times to get some coverage
        for (int i = 0; i < iters; i++) {
            try (Store store = createStore();
                 InternalEngine engine =
                     createEngine(config(defaultSettings, store, createTempDir(), new LogDocMergePolicy(), null))) {
                final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
                Engine.Index doc1 =
                    indexForDoc(testParsedDocument("1", testDocumentWithTextField(), B_1));
                engine.index(doc1);
                assertThat(doc1.startTime()).isEqualTo(engine.getLastWriteNanos());
                engine.flush();
                Engine.Index doc2 =
                    indexForDoc(testParsedDocument("2", testDocumentWithTextField(), B_1));
                engine.index(doc2);
                assertThat(doc2.startTime()).isEqualTo(engine.getLastWriteNanos());
                engine.flush();
                final boolean forceMergeFlushes = randomBoolean();
                final ParsedDocument parsedDoc3 =
                    testParsedDocument("3", testDocumentWithTextField(), B_1);
                if (forceMergeFlushes) {
                    engine.index(new Engine.Index(newUid(parsedDoc3), parsedDoc3, UNASSIGNED_SEQ_NO, 0,
                                                  Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY,
                                                  System.nanoTime() - engine.engineConfig.getFlushMergesAfter().nanos(),
                                                  -1, false, UNASSIGNED_SEQ_NO, 0));
                } else {
                    engine.index(indexForDoc(parsedDoc3));
                }
                Engine.CommitId commitID = engine.flush();
                assertThat(Engine.SyncedFlushResult.SUCCESS).as("should succeed to flush commit with right id and no pending doc").isEqualTo(engine.syncFlush(syncId, commitID));
                assertThat(engine.segments(false).size()).isEqualTo(3);

                engine.forceMerge(forceMergeFlushes, 1, false, UUIDs.randomBase64UUID());
                if (forceMergeFlushes == false) {
                    engine.refresh("make all segments visible");
                    assertThat(engine.segments(false).size()).isEqualTo(4);
                    assertThat(syncId).isEqualTo(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID));
                    assertThat(syncId).isEqualTo(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
                    assertThat(engine.tryRenewSyncCommit()).isTrue();
                    assertThat(engine.segments(false).size()).isEqualTo(1);
                } else {
                    engine.refresh("test");
                    assertBusy(() -> assertThat(engine.segments(false)).hasSize(1));
                }
                assertThat(syncId).isEqualTo(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID));
                assertThat(syncId).isEqualTo(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));

                if (randomBoolean()) {
                    Engine.Index doc4 =
                        indexForDoc(testParsedDocument("4", testDocumentWithTextField(), B_1));
                    engine.index(doc4);
                    assertThat(doc4.startTime()).isEqualTo(engine.getLastWriteNanos());
                } else {
                    Engine.Delete delete = new Engine.Delete(
                        doc1.id(),
                        doc1.uid(),
                        UNASSIGNED_SEQ_NO,
                        primaryTerm.get(),
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(),
                        UNASSIGNED_SEQ_NO,
                        0
                    );
                    engine.delete(delete);
                    assertThat(delete.startTime()).isEqualTo(engine.getLastWriteNanos());
                }
                assertThat(engine.tryRenewSyncCommit()).isFalse();
                // we might hit a concurrent flush from a finishing merge here - just wait if ongoing...
                engine.flush(false, true);
                assertThat(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID)).isNull();
                assertThat(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID)).isNull();
            }
        }
    }

    @Test
    public void testSyncedFlushSurvivesEngineRestart() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        IOUtils.close(store, engine);
        store = createStore();
        engine = createEngine(store, primaryTranslogDir, globalCheckpoint::get);
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(),
                                                new BytesArray("{}"));
        engine.index(indexForDoc(doc));
        globalCheckpoint.set(0L);
        final Engine.CommitId commitID = engine.flush();
        assertThat(Engine.SyncedFlushResult.SUCCESS).as("should succeed to flush commit with right id and no pending doc").isEqualTo(engine.syncFlush(syncId, commitID));
        assertThat(syncId).isEqualTo(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID));
        assertThat(syncId).isEqualTo(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
        EngineConfig config = engine.config();
        if (randomBoolean()) {
            engine.close();
        } else {
            engine.flushAndClose();
        }
        if (randomBoolean()) {
            final String translogUUID = Translog.createEmptyTranslog(config.getTranslogConfig().getTranslogPath(),
                                                                     UNASSIGNED_SEQ_NO, shardId, primaryTerm.get());
            store.associateIndexWithNewTranslog(translogUUID);
        }
        engine = new InternalEngine(config);
        engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        assertThat(syncId).isEqualTo(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
    }

    @Test
    public void testSyncedFlushVanishesOnReplay() throws IOException {
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1",
                                                testDocumentWithTextField(), new BytesArray("{}"));
        engine.index(indexForDoc(doc));
        final Engine.CommitId commitID = engine.flush();
        assertThat(Engine.SyncedFlushResult.SUCCESS).as("should succeed to flush commit with right id and no pending doc").isEqualTo(engine.syncFlush(syncId, commitID));
        assertThat(syncId).isEqualTo(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID));
        assertThat(syncId).isEqualTo(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
        doc = testParsedDocument("2", testDocumentWithTextField(), new BytesArray("{}"));
        engine.index(indexForDoc(doc));
        EngineConfig config = engine.config();
        engine.close();
        engine = new InternalEngine(config);
        engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        assertThat(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID))
            .as("Sync ID must be gone since we have a document to replay")
            .isNull();
    }

    @Test
    public void testVersioningNewCreate() throws IOException {
        ParsedDocument doc = testParsedDocument("1", testDocument(), B_1);
        Engine.Index create = new Engine.Index(
            newUid(doc), doc, UNASSIGNED_SEQ_NO, primaryTerm.get(),
            Versions.MATCH_DELETED, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion()).isEqualTo(1L);

        create = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), create.primaryTerm(), indexResult.getVersion(),
                                  null, REPLICA, 0, -1, false, UNASSIGNED_SEQ_NO, 0);
        indexResult = replicaEngine.index(create);
        assertThat(indexResult.getVersion()).isEqualTo(1L);
    }

    @Test
    public void testReplicatedVersioningWithFlush() throws IOException {
        ParsedDocument doc = testParsedDocument("1", testDocument(), B_1);
        Engine.Index create = new Engine.Index(
            newUid(doc), doc, UNASSIGNED_SEQ_NO, primaryTerm.get(),
            Versions.MATCH_DELETED, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion()).isEqualTo(1L);
        assertThat(indexResult.isCreated()).isTrue();


        create = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), create.primaryTerm(), indexResult.getVersion(),
                                  null, REPLICA, 0, -1, false, UNASSIGNED_SEQ_NO, 0);
        indexResult = replicaEngine.index(create);
        assertThat(indexResult.getVersion()).isEqualTo(1L);
        assertThat(indexResult.isCreated()).isTrue();

        if (randomBoolean()) {
            engine.flush();
        }
        if (randomBoolean()) {
            replicaEngine.flush();
        }

        Engine.Index update = new Engine.Index(
            newUid(doc), doc, UNASSIGNED_SEQ_NO, primaryTerm.get(),
            1, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult updateResult = engine.index(update);
        assertThat(updateResult.getVersion()).isEqualTo(2L);
        assertThat(updateResult.isCreated()).isFalse();


        update = new Engine.Index(newUid(doc), doc, updateResult.getSeqNo(), update.primaryTerm(), updateResult.getVersion(),
                                  null, REPLICA, 0, -1, false, UNASSIGNED_SEQ_NO, 0);
        updateResult = replicaEngine.index(update);
        assertThat(updateResult.getVersion()).isEqualTo(2L);
        assertThat(updateResult.isCreated()).isFalse();
        replicaEngine.refresh("test");
        try (Searcher searcher = replicaEngine.acquireSearcher("test")) {
            assertThat(searcher.getDirectoryReader().numDocs()).isEqualTo(1);
        }

        engine.refresh("test");
        try (Searcher searcher = engine.acquireSearcher("test")) {
            assertThat(searcher.getDirectoryReader().numDocs()).isEqualTo(1);
        }
    }

    /**
     * simulates what an upsert / update API does
     */
    @Test
    public void testVersionedUpdate() throws IOException {
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;

        ParsedDocument doc = testParsedDocument("1", testDocument(), B_1);
        Engine.Index create = new Engine.Index(
            newUid(doc), doc, UNASSIGNED_SEQ_NO, primaryTerm.get(),
            Versions.MATCH_DELETED, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion()).isEqualTo(1L);
        try (Engine.GetResult get = engine.get(new Engine.Get(doc.id(), create.uid()), searcherFactory)) {
            assertThat(get.docIdAndVersion().version).isEqualTo(1);
        }

        Engine.Index update_1 = new Engine.Index(
            newUid(doc), doc, UNASSIGNED_SEQ_NO, primaryTerm.get(),
            1, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult update_1_result = engine.index(update_1);
        assertThat(update_1_result.getVersion()).isEqualTo(2L);

        try (Engine.GetResult get = engine.get(new Engine.Get(doc.id(), create.uid()), searcherFactory)) {
            assertThat(get.docIdAndVersion().version).isEqualTo(2);
        }

        Engine.Index update_2 = new Engine.Index(
            newUid(doc), doc, UNASSIGNED_SEQ_NO, primaryTerm.get(),
            2, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult update_2_result = engine.index(update_2);
        assertThat(update_2_result.getVersion()).isEqualTo(3L);

        try (Engine.GetResult get = engine.get(new Engine.Get(doc.id(), create.uid()), searcherFactory)) {
            assertThat(get.docIdAndVersion().version).isEqualTo(3);
        }

    }

    @Test
    public void testVersioningNewIndex() throws IOException {
        ParsedDocument doc = testParsedDocument("1", testDocument(), B_1);
        Engine.Index index = indexForDoc(doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion()).isEqualTo(1L);

        index = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), index.primaryTerm(), indexResult.getVersion(),
                                 null, REPLICA, 0, -1, false, UNASSIGNED_SEQ_NO, 0);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion()).isEqualTo(1L);
    }

    /*
     * we are testing an edge case here where we have a fully deleted segment that is retained but has all it's IDs pruned away.
     */
    @Test
    public void testLookupVersionWithPrunedAwayIds() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Lucene.STANDARD_ANALYZER);
            indexWriterConfig.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
            try (IndexWriter writer = new IndexWriter(dir,
                                                      indexWriterConfig.setMergePolicy(new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETES_FIELD,
                                                                                                                           MatchAllDocsQuery::new, new PrunePostingsMergePolicy(indexWriterConfig.getMergePolicy(), "_id"))))) {
                org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
                doc.add(new Field(SysColumns.Names.ID, "1", SysColumns.ID.FIELD_TYPE));
                doc.add(new NumericDocValuesField(SysColumns.VERSION.name(), -1));
                doc.add(new NumericDocValuesField(SysColumns.Names.SEQ_NO, 1));
                doc.add(new NumericDocValuesField(SysColumns.Names.PRIMARY_TERM, 1));
                writer.addDocument(doc);
                writer.flush();
                writer.softUpdateDocument(new Term(SysColumns.Names.ID, "1"), doc, new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1));
                writer.updateNumericDocValue(new Term(SysColumns.Names.ID, "1"), Lucene.SOFT_DELETES_FIELD, 1);
                writer.forceMerge(1);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size()).isEqualTo(1);
                    assertThat(VersionsAndSeqNoResolver.loadDocIdAndVersion(reader, new Term(SysColumns.Names.ID, "1"), false))
                        .isNull();
                }
            }
        }
    }

    @Test
    public void testUpdateWithFullyDeletedSegments() throws IOException {
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), Integer.MAX_VALUE);
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final Set<String> liveDocs = new HashSet<>();
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), newMergePolicy(), null,
                                                         null, globalCheckpoint::get))) {
            int numDocs = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), B_1);
                engine.index(indexForDoc(doc));
                liveDocs.add(doc.id());
            }

            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), B_1);
                engine.index(indexForDoc(doc));
                liveDocs.add(doc.id());
            }
        }
    }

    @Test
    public void testForceMergeWithSoftDeletesRetention() throws Exception {
        final long retainedExtraOps = randomLongBetween(0, 10);
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), retainedExtraOps);
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final Set<String> liveDocs = new HashSet<>();
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), newMergePolicy(), null,
                                                         null, globalCheckpoint::get))) {
            int numDocs = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), B_1);
                engine.index(indexForDoc(doc));
                liveDocs.add(doc.id());
            }
            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), B_1);
                if (randomBoolean()) {
                    engine.delete(new Engine.Delete(doc.id(), newUid(doc.id()), primaryTerm.get()));
                    liveDocs.remove(doc.id());
                }
                if (randomBoolean()) {
                    engine.index(indexForDoc(doc));
                    liveDocs.add(doc.id());
                }
                if (randomBoolean()) {
                    engine.flush(randomBoolean(), true);
                }
            }
            engine.flush();

            long localCheckpoint = engine.getProcessedLocalCheckpoint();
            globalCheckpoint.set(randomLongBetween(0, localCheckpoint));
            engine.syncTranslog();
            final long safeCommitCheckpoint;
            try (Engine.IndexCommitRef safeCommit = engine.acquireSafeIndexCommit()) {
                safeCommitCheckpoint = Long.parseLong(safeCommit.getIndexCommit().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
            }
            engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
            assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
            Map<Long, Translog.Operation> ops = readAllOperationsInLucene(engine)
                .stream().collect(Collectors.toMap(Translog.Operation::seqNo, Function.identity()));
            for (long seqno = 0; seqno <= localCheckpoint; seqno++) {
                long minSeqNoToRetain = Math.min(globalCheckpoint.get() + 1 - retainedExtraOps,
                                                 safeCommitCheckpoint + 1);
                String msg = "seq# [" + seqno + "], global checkpoint [" + globalCheckpoint + "], retained-ops [" +
                             retainedExtraOps + "]";
                if (seqno < minSeqNoToRetain) {
                    Translog.Operation op = ops.get(seqno);
                    if (op != null) {
                        assertThat(op).isExactlyInstanceOf(Translog.Index.class);
                        assertThat(((Translog.Index) op).id()).as(msg).isIn(liveDocs);
                        assertThat(B_1).as(msg).isEqualTo(op.getSource());
                    }
                } else {
                    assertThat(ops.get(seqno))
                        .as(msg)
                        .isNotNull();
                }
            }
            settings.put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0);
            indexSettings.updateIndexMetadata(IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(
                settings).build());
            engine.onSettingsChanged(indexSettings.getTranslogRetentionAge(), indexSettings.getTranslogRetentionSize(),
                                     indexSettings.getSoftDeleteRetentionOperations());
            globalCheckpoint.set(localCheckpoint);
            engine.syncTranslog();

            engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
            assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
            assertThat(readAllOperationsInLucene(engine)).hasSize(liveDocs.size());
        }
    }

    @Test
    public void testForceMergeWithSoftDeletesRetentionAndRecoverySource() throws Exception {
        final long retainedExtraOps = randomLongBetween(0, 10);
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), retainedExtraOps);
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final boolean omitSourceAllTheTime = randomBoolean();
        final Set<String> liveDocs = new HashSet<>();
        final Set<String> liveDocsWithSource = new HashSet<>();
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), newMergePolicy(), null,
                                                         null,
                                                         globalCheckpoint::get))) {
            int numDocs = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                boolean useRecoverySource = randomBoolean() || omitSourceAllTheTime;
                ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), B_1,
                    useRecoverySource);
                engine.index(indexForDoc(doc));
                liveDocs.add(doc.id());
                if (useRecoverySource == false) {
                    liveDocsWithSource.add(Integer.toString(i));
                }
            }
            for (int i = 0; i < numDocs; i++) {
                boolean useRecoverySource = randomBoolean() || omitSourceAllTheTime;
                ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), B_1,
                    useRecoverySource);
                if (randomBoolean()) {
                    engine.delete(new Engine.Delete(doc.id(), newUid(doc.id()), primaryTerm.get()));
                    liveDocs.remove(doc.id());
                    liveDocsWithSource.remove(doc.id());
                }
                if (randomBoolean()) {
                    engine.index(indexForDoc(doc));
                    liveDocs.add(doc.id());
                    if (useRecoverySource == false) {
                        liveDocsWithSource.add(doc.id());
                    } else {
                        liveDocsWithSource.remove(doc.id());
                    }
                }
                if (randomBoolean()) {
                    engine.flush(randomBoolean(), true);
                }
            }
            engine.flush();
            globalCheckpoint.set(randomLongBetween(0, engine.getPersistedLocalCheckpoint()));
            engine.syncTranslog();
            final long minSeqNoToRetain;
            try (Engine.IndexCommitRef safeCommit = engine.acquireSafeIndexCommit()) {
                long safeCommitLocalCheckpoint = Long.parseLong(
                    safeCommit.getIndexCommit().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
                minSeqNoToRetain = Math.min(globalCheckpoint.get() + 1 - retainedExtraOps,
                                            safeCommitLocalCheckpoint + 1);
            }
            engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
            assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
            Map<Long, Translog.Operation> ops = readAllOperationsInLucene(engine)
                .stream().collect(Collectors.toMap(Translog.Operation::seqNo, Function.identity()));
            for (long seqno = 0; seqno <= engine.getPersistedLocalCheckpoint(); seqno++) {
                String msg = "seq# [" + seqno + "], global checkpoint [" + globalCheckpoint + "], retained-ops [" +
                             retainedExtraOps + "]";
                if (seqno < minSeqNoToRetain) {
                    Translog.Operation op = ops.get(seqno);
                    if (op != null) {
                        assertThat(op).isExactlyInstanceOf(Translog.Index.class);
                        assertThat(((Translog.Index) op).id()).as(msg).isIn(liveDocs);
                    }
                } else {
                    Translog.Operation op = ops.get(seqno);
                    assertThat(op).as(msg).isNotNull();
                    if (op instanceof Translog.Index) {
                        assertThat(B_1).as(msg).isEqualTo(((Translog.Index) op).getSource());
                    }
                }
            }
            settings.put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0);
            indexSettings.updateIndexMetadata(IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(
                settings).build());
            engine.onSettingsChanged(indexSettings.getTranslogRetentionAge(), indexSettings.getTranslogRetentionSize(),
                                     indexSettings.getSoftDeleteRetentionOperations());
            // If we already merged down to 1 segment, then the next force-merge will be a noop. We need to add an extra segment to make
            // merges happen so we can verify that _recovery_source are pruned. See: https://github.com/elastic/elasticsearch/issues/41628.
            final int numSegments;
            try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                numSegments = searcher.getDirectoryReader().leaves().size();
            }
            if (numSegments == 1) {
                boolean useRecoverySource = randomBoolean() || omitSourceAllTheTime;
                ParsedDocument doc = testParsedDocument("dummy", testDocument(), B_1, useRecoverySource);
                engine.index(indexForDoc(doc));
                if (useRecoverySource == false) {
                    liveDocsWithSource.add(doc.id());
                }
                engine.syncTranslog();
                globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                engine.flush(randomBoolean(), true);
            } else {
                globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                engine.syncTranslog();
            }
            engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
            assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
            assertThat(readAllOperationsInLucene(engine)).hasSize(liveDocsWithSource.size());
        }
    }


    @Test
    public void testForceMergeAndClose() throws IOException, InterruptedException {
        int numIters = randomIntBetween(2, 10);
        for (int j = 0; j < numIters; j++) {
            try (Store store = createStore()) {
                final InternalEngine engine = createEngine(store, createTempDir());
                final CountDownLatch startGun = new CountDownLatch(1);
                final CountDownLatch indexed = new CountDownLatch(1);

                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            try {
                                startGun.await();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            int i = 0;
                            while (true) {
                                int numDocs = randomIntBetween(1, 20);
                                for (int j = 0; j < numDocs; j++) {
                                    i++;
                                    ParsedDocument doc = testParsedDocument(Integer.toString(i),  testDocument(), B_1
                                    );
                                    Engine.Index index = indexForDoc(doc);
                                    engine.index(index);
                                }
                                engine.refresh("test");
                                indexed.countDown();
                                try {
                                    engine.forceMerge(
                                        randomBoolean(),
                                        1,
                                        false,
                                        UUIDs.randomBase64UUID()
                                    );
                                } catch (IOException e) {
                                    return;
                                }
                            }
                        } catch (AlreadyClosedException ex) {
                            // fine
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                };

                thread.start();
                startGun.countDown();
                int someIters = randomIntBetween(1, 10);
                for (int i = 0; i < someIters; i++) {
                    engine.forceMerge(randomBoolean(), 1, false, UUIDs.randomBase64UUID());
                }
                indexed.await();
                IOUtils.close(engine);
                thread.join();
            }
        }

    }

    @Test
    public void testVersioningCreateExistsException() throws IOException {
        ParsedDocument doc = testParsedDocument("1", testDocument(), B_1);
        Engine.Index create = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 1,
                                               Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion()).isEqualTo(1L);

        create = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 1, Versions.MATCH_DELETED,
                                  VersionType.INTERNAL, PRIMARY, 0, -1, false, UNASSIGNED_SEQ_NO, 0);
        indexResult = engine.index(create);
        assertThat(indexResult.getResultType()).isEqualTo(Engine.Result.Type.FAILURE);
        assertThat(indexResult.getFailure()).isExactlyInstanceOf(VersionConflictEngineException.class);
    }

    @Test
    public void testOutOfOrderDocsOnReplica() throws IOException {
        final List<Engine.Operation> ops = generateSingleDocHistory(
            true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL, VersionType.EXTERNAL_GTE),
            2, 2, 20, "1");
        assertOpsOnReplica(ops, replicaEngine, true, logger);
    }

    @Test
    public void testConcurrentOutOfOrderDocsOnReplica() throws IOException, InterruptedException {
        final List<Engine.Operation> opsDoc1 = generateSingleDocHistory(
            true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), 2, 100, 300, "1");
        final Engine.Operation lastOpDoc1 = opsDoc1.get(opsDoc1.size() - 1);
        final String lastFieldValueDoc1;
        if (lastOpDoc1 instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOpDoc1;
            lastFieldValueDoc1 = index.document().get("value");
        } else {
            // delete
            lastFieldValueDoc1 = null;
        }
        final List<Engine.Operation> opsDoc2 =
            generateSingleDocHistory(
                true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), 2, 100, 300, "2");
        final Engine.Operation lastOpDoc2 = opsDoc2.get(opsDoc2.size() - 1);
        final String lastFieldValueDoc2;
        if (lastOpDoc2 instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOpDoc2;
            lastFieldValueDoc2 = index.document().get("value");
        } else {
            // delete
            lastFieldValueDoc2 = null;
        }
        // randomly interleave
        final AtomicLong seqNoGenerator = new AtomicLong();
        BiFunction<Engine.Operation, Long, Engine.Operation> seqNoUpdater = (operation, newSeqNo) -> {
            if (operation instanceof Engine.Index) {
                Engine.Index index = (Engine.Index) operation;
                Document doc = testDocumentWithTextField(index.document().get("value"));
                ParsedDocument parsedDocument = testParsedDocument(index.id(), doc, index.source());
                return new Engine.Index(index.uid(), parsedDocument, newSeqNo, index.primaryTerm(), index.version(),
                                        index.versionType(), index.origin(), index.startTime(), index.getAutoGeneratedIdTimestamp(), index.isRetry(),
                                        UNASSIGNED_SEQ_NO, 0);
            } else {
                Engine.Delete delete = (Engine.Delete) operation;
                return new Engine.Delete(
                    delete.id(),
                    delete.uid(),
                    newSeqNo,
                    delete.primaryTerm(),
                    delete.version(),
                    delete.versionType(),
                    delete.origin(),
                    delete.startTime(),
                    UNASSIGNED_SEQ_NO,
                    0
                );
            }
        };
        final List<Engine.Operation> allOps = new ArrayList<>();
        Iterator<Engine.Operation> iter1 = opsDoc1.iterator();
        Iterator<Engine.Operation> iter2 = opsDoc2.iterator();
        while (iter1.hasNext() && iter2.hasNext()) {
            final Engine.Operation next = randomBoolean() ? iter1.next() : iter2.next();
            allOps.add(seqNoUpdater.apply(next, seqNoGenerator.getAndIncrement()));
        }
        iter1.forEachRemaining(o -> allOps.add(seqNoUpdater.apply(o, seqNoGenerator.getAndIncrement())));
        iter2.forEachRemaining(o -> allOps.add(seqNoUpdater.apply(o, seqNoGenerator.getAndIncrement())));
        // insert some duplicates
        randomSubsetOf(allOps).forEach(op -> allOps.add(seqNoUpdater.apply(op, op.seqNo())));

        shuffle(allOps, random());
        concurrentlyApplyOps(allOps, engine);

        engine.refresh("test");

        if (lastFieldValueDoc1 != null) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(new TermQuery(new Term("value", lastFieldValueDoc1)), collector);
                assertThat(collector.getTotalHits()).isEqualTo(1);
            }
        }
        if (lastFieldValueDoc2 != null) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(new TermQuery(new Term("value", lastFieldValueDoc2)), collector);
                assertThat(collector.getTotalHits()).isEqualTo(1);
            }
        }

        int totalExpectedOps = 0;
        if (lastFieldValueDoc1 != null) {
            totalExpectedOps++;
        }
        if (lastFieldValueDoc2 != null) {
            totalExpectedOps++;
        }
        assertVisibleCount(engine, totalExpectedOps);
    }

    @Test
    public void testInternalVersioningOnPrimary() throws IOException {
        final List<Engine.Operation> ops = generateSingleDocHistory(
            false, VersionType.INTERNAL, 2, 2, 20, "1");
        assertOpsOnPrimary(ops, Versions.NOT_FOUND, true, engine);
    }

    @Test
    public void testVersionOnPrimaryWithConcurrentRefresh() throws Exception {
        List<Engine.Operation> ops = generateSingleDocHistory(
            false, VersionType.INTERNAL, 2, 10, 100, "1");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);
        Thread refreshThread = new Thread(() -> {
            latch.countDown();
            while (running.get()) {
                engine.refresh("test");
            }
        });
        refreshThread.start();
        try {
            latch.await();
            assertOpsOnPrimary(ops, Versions.NOT_FOUND, true, engine);
        } finally {
            running.set(false);
            refreshThread.join();
        }
    }

    private int assertOpsOnPrimary(List<Engine.Operation> ops,
                                   long currentOpVersion,
                                   boolean docDeleted,
                                   InternalEngine engine)
        throws IOException {
        String lastFieldValue = null;
        int opsPerformed = 0;
        long lastOpVersion = currentOpVersion;
        long lastOpSeqNo = UNASSIGNED_SEQ_NO;
        long lastOpTerm = UNASSIGNED_PRIMARY_TERM;
        PrimaryTermSupplier currentTerm = (PrimaryTermSupplier) engine.engineConfig.getPrimaryTermSupplier();
        BiFunction<Long, Engine.Index, Engine.Index> indexWithVersion = (version, index) -> new Engine.Index(
            index.uid(),
            index.parsedDoc(),
            UNASSIGNED_SEQ_NO,
            currentTerm.get(),
            version,
            index.versionType(),
            index.origin(),
            index.startTime(),
            index.getAutoGeneratedIdTimestamp(),
            index.isRetry(),
            UNASSIGNED_SEQ_NO,
            0);
        BiFunction<Long, Engine.Delete, Engine.Delete> delWithVersion = (version, delete) -> new Engine.Delete(
            delete.id(),
            delete.uid(),
            UNASSIGNED_SEQ_NO,
            currentTerm.get(),
            version,
            delete.versionType(),
            delete.origin(),
            delete.startTime(),
            UNASSIGNED_SEQ_NO,
            0);
        TriFunction<Long, Long, Engine.Index, Engine.Index> indexWithSeq = (seqNo, term, index) -> new Engine.Index(
            index.uid(),
            index.parsedDoc(),
            UNASSIGNED_SEQ_NO,
            currentTerm.get(),
            index.version(),
            index.versionType(),
            index.origin(),
            index.startTime(),
            index.getAutoGeneratedIdTimestamp(),
            index.isRetry(),
            seqNo,
            term);
        TriFunction<Long, Long, Engine.Delete, Engine.Delete> delWithSeq = (seqNo, term, delete) -> new Engine.Delete(
            delete.id(),
            delete.uid(),
            UNASSIGNED_SEQ_NO,
            currentTerm.get(),
            delete.version(),
            delete.versionType(),
            delete.origin(),
            delete.startTime(),
            seqNo,
            term);
        UnaryOperator<Engine.Index> indexWithCurrentTerm = index -> new Engine.Index(
            index.uid(),
            index.parsedDoc(),
            UNASSIGNED_SEQ_NO,
            currentTerm.get(),
            index.version(),
            index.versionType(),
            index.origin(),
            index.startTime(),
            index.getAutoGeneratedIdTimestamp(),
            index.isRetry(),
            index.getIfSeqNo(),
            index.getIfPrimaryTerm());
        UnaryOperator<Engine.Delete> deleteWithCurrentTerm = delete -> new Engine.Delete(
            delete.id(),
            delete.uid(),
            UNASSIGNED_SEQ_NO,
            currentTerm.get(),
            delete.version(),
            delete.versionType(),
            delete.origin(),
            delete.startTime(),
            delete.getIfSeqNo(),
            delete.getIfPrimaryTerm());
        for (Engine.Operation op : ops) {
            final boolean versionConflict = rarely();
            final boolean versionedOp = versionConflict || randomBoolean();
            final long conflictingVersion = docDeleted || randomBoolean() ?
                lastOpVersion + (randomBoolean() ? 1 : -1) :
                Versions.MATCH_DELETED;
            final long conflictingSeqNo = lastOpSeqNo == UNASSIGNED_SEQ_NO  || randomBoolean() ?
                lastOpSeqNo + 5 : // use 5 to go above 0 for magic numbers
                lastOpSeqNo;
            final long conflictingTerm = conflictingSeqNo == lastOpSeqNo || randomBoolean() ? lastOpTerm + 1 : lastOpTerm;
            if (rarely()) {
                currentTerm.set(currentTerm.get() + 1L);
                engine.rollTranslogGeneration();
            }
            final long correctVersion = docDeleted ? Versions.MATCH_DELETED : lastOpVersion;
            logger.info("performing [{}]{}{}",
                        op.operationType().name().charAt(0),
                        versionConflict ? " (conflict " + conflictingVersion + ")" : "",
                        versionedOp ? " (versioned " + correctVersion + ", seqNo " + lastOpSeqNo + ", term " + lastOpTerm + " )" : "");
            if (op instanceof Engine.Index) {
                final Engine.Index index = (Engine.Index) op;
                if (versionConflict) {
                    // generate a conflict
                    final Engine.IndexResult result;
                    if (randomBoolean()) {
                        result = engine.index(indexWithSeq.apply(conflictingSeqNo, conflictingTerm, index));
                    } else {
                        result = engine.index(indexWithVersion.apply(conflictingVersion, index));
                    }
                    assertThat(result.isCreated()).isFalse();
                    assertThat(result.getVersion()).isEqualTo(lastOpVersion);
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.FAILURE);
                    assertThat(result.getFailure()).isExactlyInstanceOf(VersionConflictEngineException.class);
                } else {
                    final Engine.IndexResult result;
                    if (versionedOp) {
                        // TODO: add support for non-existing docs
                        if (randomBoolean() && lastOpSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO && docDeleted == false) {
                            result = engine.index(indexWithSeq.apply(lastOpSeqNo, lastOpTerm, index));
                        } else {
                            result = engine.index(indexWithVersion.apply(correctVersion, index));
                        }
                    } else {
                        result = engine.index(indexWithCurrentTerm.apply(index));
                    }
                    assertThat(result.isCreated()).isEqualTo(docDeleted);
                    assertThat(result.getVersion()).isEqualTo(Math.max(lastOpVersion + 1, 1));
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.SUCCESS);
                    assertThat(result.getFailure()).isNull();
                    lastFieldValue = index.document().get("value");
                    assert lastFieldValue != null : "lastFieldValue is null after getting it from index docs";
                    docDeleted = false;
                    lastOpVersion = result.getVersion();
                    lastOpSeqNo = result.getSeqNo();
                    lastOpTerm = result.getTerm();
                    opsPerformed++;
                }
            } else {
                final Engine.Delete delete = (Engine.Delete) op;
                if (versionConflict) {
                    // generate a conflict
                    Engine.DeleteResult result;
                    if (randomBoolean()) {
                        result = engine.delete(delWithSeq.apply(conflictingSeqNo, conflictingTerm, delete));
                    } else {
                        result = engine.delete(delWithVersion.apply(conflictingVersion, delete));
                    }
                    assertThat(result.isFound()).isEqualTo(docDeleted == false);
                    assertThat(result.getVersion()).isEqualTo(lastOpVersion);
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.FAILURE);
                    assertThat(result.getFailure()).isExactlyInstanceOf(VersionConflictEngineException.class);
                } else {
                    final Engine.DeleteResult result;
                    long correctSeqNo = docDeleted ? UNASSIGNED_SEQ_NO : lastOpSeqNo;
                    if (versionedOp && lastOpSeqNo != UNASSIGNED_SEQ_NO && randomBoolean()) {
                        result = engine.delete(delWithSeq.apply(correctSeqNo, lastOpTerm, delete));
                    } else if (versionedOp) {
                        result = engine.delete(delWithVersion.apply(correctVersion, delete));
                    } else {
                        result = engine.delete(deleteWithCurrentTerm.apply(delete));
                    }
                    assertThat(result.isFound()).isEqualTo(docDeleted == false);
                    assertThat(result.getVersion()).isEqualTo(Math.max(lastOpVersion + 1, 1));
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.SUCCESS);
                    assertThat(result.getFailure()).isNull();
                    docDeleted = true;
                    lastOpVersion = result.getVersion();
                    lastOpSeqNo = result.getSeqNo();
                    lastOpTerm = result.getTerm();
                    opsPerformed++;
                }
            }
            if (randomBoolean()) {
                // refresh and take the chance to check everything is ok so far
                assertVisibleCount(engine, docDeleted ? 0 : 1);
                // even if doc is not not deleted, lastFieldValue can still be null if this is the
                // first op and it failed.
                if (docDeleted == false && lastFieldValue != null) {
                    try (Searcher searcher = engine.acquireSearcher("test")) {
                        final TotalHitCountCollector collector = new TotalHitCountCollector();
                        searcher.search(new TermQuery(new Term("value", lastFieldValue)), collector);
                        assertThat(collector.getTotalHits()).isEqualTo(1);
                    }
                }
            }
            if (randomBoolean()) {
                engine.flush();
                engine.refresh("test");
            }

            if (rarely()) {
                // simulate GC deletes
                engine.refresh("gc_simulation", Engine.SearcherScope.INTERNAL, true);
                engine.clearDeletedTombstones();
                if (docDeleted) {
                    lastOpVersion = Versions.NOT_FOUND;
                    lastOpSeqNo = UNASSIGNED_SEQ_NO;
                    lastOpTerm = UNASSIGNED_PRIMARY_TERM;
                }
            }
        }

        assertVisibleCount(engine, docDeleted ? 0 : 1);
        if (docDeleted == false) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                // lastFieldValue can be null if all index ops had a version conflict
                if (lastFieldValue != null) {
                    searcher.search(new TermQuery(new Term("value", lastFieldValue)), collector);
                    assertThat(collector.getTotalHits()).isEqualTo(1);
                }
            }
        }
        return opsPerformed;
    }

    @Test
    public void testNonInternalVersioningOnPrimary() throws IOException {
        final Set<VersionType> nonInternalVersioning = new HashSet<>(Arrays.asList(VersionType.values()));
        nonInternalVersioning.remove(VersionType.INTERNAL);
        final VersionType versionType = randomFrom(nonInternalVersioning);
        final List<Engine.Operation> ops = generateSingleDocHistory(
            false, versionType, 2, 2, 20, "1");
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.document().get("value");
        } else {
            // delete
            lastFieldValue = null;
        }
        // other version types don't support out of order processing.
        if (versionType == VersionType.EXTERNAL) {
            shuffle(ops, random());
        }
        long highestOpVersion = Versions.NOT_FOUND;
        long seqNo = -1;
        boolean docDeleted = true;
        for (Engine.Operation op : ops) {
            logger.info("performing [{}], v [{}], seq# [{}], term [{}]",
                        op.operationType().name().charAt(0), op.version(), op.seqNo(), op.primaryTerm());
            if (op instanceof Engine.Index) {
                final Engine.Index index = (Engine.Index) op;
                Engine.IndexResult result = engine.index(index);
                if (op.versionType().isVersionConflictForWrites(highestOpVersion, op.version(), docDeleted) == false) {
                    seqNo++;
                    assertThat(result.getSeqNo()).isEqualTo(seqNo);
                    assertThat(result.isCreated()).isEqualTo(docDeleted);
                    assertThat(result.getVersion()).isEqualTo(op.version());
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.SUCCESS);
                    assertThat(result.getFailure()).isNull();
                    docDeleted = false;
                    highestOpVersion = op.version();
                } else {
                    assertThat(result.isCreated()).isFalse();
                    assertThat(result.getVersion()).isEqualTo(highestOpVersion);
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.FAILURE);
                    assertThat(result.getFailure()).isExactlyInstanceOf(VersionConflictEngineException.class);
                }
            } else {
                final Engine.Delete delete = (Engine.Delete) op;
                Engine.DeleteResult result = engine.delete(delete);
                if (op.versionType().isVersionConflictForWrites(highestOpVersion, op.version(), docDeleted) == false) {
                    seqNo++;
                    assertThat(result.getSeqNo()).isEqualTo(seqNo);
                    assertThat(result.isFound()).isEqualTo(docDeleted == false);
                    assertThat(result.getVersion()).isEqualTo(op.version());
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.SUCCESS);
                    assertThat(result.getFailure()).isNull();
                    docDeleted = true;
                    highestOpVersion = op.version();
                } else {
                    assertThat(result.isFound()).isEqualTo(docDeleted == false);
                    assertThat(result.getVersion()).isEqualTo(highestOpVersion);
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.FAILURE);
                    assertThat(result.getFailure()).isExactlyInstanceOf(VersionConflictEngineException.class);
                }
            }
            if (randomBoolean()) {
                engine.refresh("test");
            }
            if (randomBoolean()) {
                engine.flush();
                engine.refresh("test");
            }
        }

        assertVisibleCount(engine, docDeleted ? 0 : 1);
        if (docDeleted == false) {
            logger.info("searching for [{}]", lastFieldValue);
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits()).isEqualTo(1);
            }
        }
    }

    @Test
    public void testVersioningPromotedReplica() throws IOException {
        final List<Engine.Operation> replicaOps = generateSingleDocHistory(
            true, VersionType.INTERNAL, 1, 2, 20, "1");
        List<Engine.Operation> primaryOps = generateSingleDocHistory(
            false, VersionType.INTERNAL, 2, 2, 20, "1");
        Engine.Operation lastReplicaOp = replicaOps.get(replicaOps.size() - 1);
        final boolean deletedOnReplica = lastReplicaOp instanceof Engine.Delete;
        final long finalReplicaVersion = lastReplicaOp.version();
        final long finalReplicaSeqNo = lastReplicaOp.seqNo();
        assertOpsOnReplica(replicaOps, replicaEngine, true, logger);
        final int opsOnPrimary = assertOpsOnPrimary(primaryOps, finalReplicaVersion, deletedOnReplica, replicaEngine);
        final long currentSeqNo = getSequenceID(
            replicaEngine,
            new Engine.Get(lastReplicaOp.uid().text(), lastReplicaOp.uid())).v1();
        try (Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.search(new MatchAllDocsQuery(), collector);
            if (collector.getTotalHits() > 0) {
                // last op wasn't delete
                assertThat(currentSeqNo).isEqualTo(finalReplicaSeqNo + opsOnPrimary);
            }
        }
    }

    @Test
    public void testConcurrentExternalVersioningOnPrimary() throws IOException, InterruptedException {
        final List<Engine.Operation> ops = generateSingleDocHistory(
            false, VersionType.EXTERNAL, 2, 100, 300, "1");
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.document().get("value");
        } else {
            // delete
            lastFieldValue = null;
        }
        shuffle(ops, random());
        concurrentlyApplyOps(ops, engine);

        assertVisibleCount(engine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits()).isEqualTo(1);
            }
        }
    }

    @Test
    public void testConcurrentGetAndSetOnPrimary() throws IOException, InterruptedException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        CountDownLatch startGun = new CountDownLatch(thread.length);
        final int opsPerThread = randomIntBetween(10, 20);
        class OpAndVersion {
            final long version;
            final String removed;
            final String added;

            OpAndVersion(long version, String removed, String added) {
                this.version = version;
                this.removed = removed;
                this.added = added;
            }
        }
        final AtomicInteger idGenerator = new AtomicInteger();
        final Queue<OpAndVersion> history = new ConcurrentLinkedQueue<>();
        ParsedDocument doc = testParsedDocument("1", testDocument(), bytesArray(""));
        final Term uidTerm = newUid(doc);
        engine.index(indexForDoc(doc));
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread(() -> {
                startGun.countDown();
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                for (int op = 0; op < opsPerThread; op++) {
                    try (Engine.GetResult get = engine.get(new Engine.Get(doc.id(), uidTerm), searcherFactory)) {
                        FieldsVisitor visitor = new FieldsVisitor(true);
                        get.docIdAndVersion().reader.document(get.docIdAndVersion().docId, visitor);
                        List<String> values = new ArrayList<>(Strings.commaDelimitedListToSet(visitor.source().utf8ToString()));
                        String removed = op % 3 == 0 && values.size() > 0 ? values.remove(0) : null;
                        String added = "v_" + idGenerator.incrementAndGet();
                        values.add(added);
                        Engine.Index index = new Engine.Index(uidTerm,
                                                              testParsedDocument("1", testDocument(),
                                                                                 bytesArray(String.join(",", values))),
                                                              UNASSIGNED_SEQ_NO, 2,
                                                              get.docIdAndVersion().version, VersionType.INTERNAL,
                                                              PRIMARY, System.currentTimeMillis(), -1, false, UNASSIGNED_SEQ_NO, 0);
                        Engine.IndexResult indexResult = engine.index(index);
                        if (indexResult.getResultType() == Engine.Result.Type.SUCCESS) {
                            history.add(new OpAndVersion(indexResult.getVersion(), removed, added));
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
        List<OpAndVersion> sortedHistory = new ArrayList<>(history);
        sortedHistory.sort(Comparator.comparing(o -> o.version));
        Set<String> currentValues = new HashSet<>();
        for (int i = 0; i < sortedHistory.size(); i++) {
            OpAndVersion op = sortedHistory.get(i);
            if (i > 0) {
                assertThat(op.version).as("duplicate version").isNotEqualTo(sortedHistory.get(i - 1).version);
            }
            boolean exists = op.removed == null ? true : currentValues.remove(op.removed);
            assertThat(exists).as(op.removed + " should exist").isTrue();
            exists = currentValues.add(op.added);
            assertThat(exists).as(op.added + " should not exist").isTrue();
        }

        try (Engine.GetResult get = engine.get(new Engine.Get(doc.id(), uidTerm), searcherFactory)) {
            FieldsVisitor visitor = new FieldsVisitor(true);
            get.docIdAndVersion().reader.document(get.docIdAndVersion().docId, visitor);
            List<String> values = Arrays.asList(Strings.commaDelimitedListToStringArray(visitor.source().utf8ToString()));
            assertThat(currentValues).isEqualTo(new HashSet<>(values));
        }
    }

    @Test
    public void testBasicCreatedFlag() throws IOException {
        ParsedDocument doc = testParsedDocument("1", testDocument(), B_1);
        Engine.Index index = indexForDoc(doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.isCreated()).isTrue();

        index = indexForDoc(doc);
        indexResult = engine.index(index);
        assertThat(indexResult.isCreated()).isFalse();

        engine.delete(new Engine.Delete(
            "1",
            newUid(doc),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            UNASSIGNED_SEQ_NO,
            0
        ));

        index = indexForDoc(doc);
        indexResult = engine.index(index);
        assertThat(indexResult.isCreated()).isTrue();
    }

    private static class MockAppender extends AbstractAppender {
        public boolean sawIndexWriterMessage;

        public boolean sawIndexWriterIFDMessage;

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0],
                                                 false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            final String formattedMessage = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.TRACE && event.getMarker().getName().contains("[index][0]")) {
                if (event.getLoggerName().endsWith(".IW") &&
                    formattedMessage.contains("IW: now apply all deletes")) {
                    sawIndexWriterMessage = true;
                }
                if (event.getLoggerName().endsWith(".IFD")) {
                    sawIndexWriterIFDMessage = true;
                }
            }
        }
    }

    // #5891: make sure IndexWriter's infoStream output is
    // sent to lucene.iw with log level TRACE:
    @Test
    public void testIndexWriterInfoStream() throws IllegalAccessException, IOException {
        LuceneTestCase.assumeFalse("who tests the tester?", LuceneTestCase.VERBOSE);
        MockAppender mockAppender = new MockAppender("testIndexWriterInfoStream");
        mockAppender.start();

        Logger rootLogger = LogManager.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        Loggers.addAppender(rootLogger, mockAppender);
        Loggers.setLevel(rootLogger, Level.DEBUG);
        rootLogger = LogManager.getRootLogger();

        try {
            // First, with DEBUG, which should NOT log IndexWriter output:
            ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), B_1);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertThat(mockAppender.sawIndexWriterMessage).isFalse();

            // Again, with TRACE, which should log IndexWriter output:
            Loggers.setLevel(rootLogger, Level.TRACE);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertThat(mockAppender.sawIndexWriterMessage).isTrue();

        } finally {
            Loggers.removeAppender(rootLogger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(rootLogger, savedLevel);
        }
    }

    @Test
    public void testSeqNoAndCheckpoints() throws IOException, InterruptedException {
        final int opCount = randomIntBetween(1, 256);
        long primarySeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final String[] ids = new String[]{"1", "2", "3"};
        final Set<String> indexedIds = new HashSet<>();
        long localCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
        long replicaLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
        final long globalCheckpoint;
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        IOUtils.close(store, engine);
        store = createStore();
        InternalEngine initialEngine = null;

        try {
            initialEngine = createEngine(defaultSettings, store, createTempDir(), newLogMergePolicy(), null);
            final ShardRouting primary = TestShardRouting.newShardRouting("test",
                                                                          shardId.id(), "node1", null, true,
                                                                          ShardRoutingState.STARTED, allocationId);
            final ShardRouting initializingReplica =
                TestShardRouting.newShardRouting(shardId, "node2", false, ShardRoutingState.INITIALIZING);
            ReplicationTracker gcpTracker = (ReplicationTracker) initialEngine.config().getGlobalCheckpointSupplier();
            gcpTracker.updateFromMaster(1L, new HashSet<>(Collections.singletonList(primary.allocationId().getId())),
                new IndexShardRoutingTable.Builder(shardId).addShard(primary).build());
            gcpTracker.activatePrimaryMode(primarySeqNo);
            if (defaultSettings.isSoftDeleteEnabled()) {
                final CountDownLatch countDownLatch = new CountDownLatch(1);
                gcpTracker.addPeerRecoveryRetentionLease(initializingReplica.currentNodeId(),
                    SequenceNumbers.NO_OPS_PERFORMED, ActionListener.wrap(countDownLatch::countDown));
                countDownLatch.await(5, TimeUnit.SECONDS);
            }
            gcpTracker.updateFromMaster(2L, new HashSet<>(Collections.singletonList(primary.allocationId().getId())),
                new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(initializingReplica).build());
            gcpTracker.initiateTracking(initializingReplica.allocationId().getId());
            gcpTracker.markAllocationIdAsInSync(initializingReplica.allocationId().getId(), replicaLocalCheckpoint);
            final ShardRouting replica = initializingReplica.moveToStarted();
            gcpTracker.updateFromMaster(3L, new HashSet<>(Arrays.asList(primary.allocationId().getId(), replica.allocationId().getId())),
                new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(replica).build());

            for (int op = 0; op < opCount; op++) {
                final String id;
                // mostly index, sometimes delete
                if (rarely() && indexedIds.isEmpty() == false) {
                    // we have some docs indexed, so delete one of them
                    id = randomFrom(indexedIds);
                    final Engine.Delete delete = new Engine.Delete(
                        id,
                        newUid(id),
                        UNASSIGNED_SEQ_NO,
                        primaryTerm.get(),
                        rarely() ? 100 : Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        PRIMARY,
                        System.nanoTime(),
                        UNASSIGNED_SEQ_NO,
                        0
                    );
                    final Engine.DeleteResult result = initialEngine.delete(delete);
                    if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                        assertThat(result.getSeqNo()).isEqualTo(primarySeqNo + 1);
                        assertThat(initialEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(primarySeqNo + 1);
                        indexedIds.remove(id);
                        primarySeqNo++;
                    } else {
                        assertThat(result.getSeqNo()).isEqualTo(UNASSIGNED_SEQ_NO);
                        assertThat(initialEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(primarySeqNo);
                    }
                } else {
                    // index a document
                    id = randomFrom(ids);
                    ParsedDocument doc = testParsedDocument(id, testDocumentWithTextField(), SOURCE);
                    final Engine.Index index = new Engine.Index(newUid(doc), doc,
                                                                UNASSIGNED_SEQ_NO, primaryTerm.get(),
                                                                rarely() ? 100 : Versions.MATCH_ANY, VersionType.INTERNAL,
                                                                PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
                    final Engine.IndexResult result = initialEngine.index(index);
                    if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                        assertThat(result.getSeqNo()).isEqualTo(primarySeqNo + 1);
                        assertThat(initialEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(primarySeqNo + 1);
                        indexedIds.add(id);
                        primarySeqNo++;
                    } else {
                        assertThat(result.getSeqNo()).isEqualTo(UNASSIGNED_SEQ_NO);
                        assertThat(initialEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(primarySeqNo);
                    }
                }

                initialEngine.syncTranslog(); // to advance persisted local checkpoint

                if (randomInt(10) < 3) {
                    // only update rarely as we do it every doc
                    replicaLocalCheckpoint = randomIntBetween(Math.toIntExact(replicaLocalCheckpoint), Math.toIntExact(primarySeqNo));
                }
                gcpTracker.updateLocalCheckpoint(primary.allocationId().getId(),
                    initialEngine.getPersistedLocalCheckpoint());
                gcpTracker.updateLocalCheckpoint(initializingReplica.allocationId().getId(), replicaLocalCheckpoint);

                if (rarely()) {
                    localCheckpoint = primarySeqNo;
                    maxSeqNo = primarySeqNo;
                    initialEngine.flush(true, true);
                }
            }

            logger.info("localcheckpoint {}, global {}", replicaLocalCheckpoint, primarySeqNo);
            globalCheckpoint = gcpTracker.getGlobalCheckpoint();

            assertThat(initialEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(primarySeqNo);
            assertThat(initialEngine.getPersistedLocalCheckpoint()).isEqualTo(primarySeqNo);
            assertThat(globalCheckpoint).isEqualTo(replicaLocalCheckpoint);

            assertThat(
                Long.parseLong(initialEngine.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY))).isEqualTo(localCheckpoint);
            initialEngine.getTranslog().sync(); // to guarantee the global checkpoint is written to the translog checkpoint
            assertThat(
                initialEngine.getTranslog().getLastSyncedGlobalCheckpoint()).isEqualTo(globalCheckpoint);
            assertThat(
                Long.parseLong(initialEngine.commitStats().getUserData().get(SequenceNumbers.MAX_SEQ_NO))).isEqualTo(maxSeqNo);

        } finally {
            IOUtils.close(initialEngine);
        }

        try (InternalEngine recoveringEngine = new InternalEngine(initialEngine.config())) {
            recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);

            assertThat(recoveringEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(primarySeqNo);
            assertThat(
                Long.parseLong(recoveringEngine.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY))).isEqualTo(primarySeqNo);
            assertThat(
                recoveringEngine.getTranslog().getLastSyncedGlobalCheckpoint()).isEqualTo(globalCheckpoint);
            assertThat(
                Long.parseLong(recoveringEngine.commitStats().getUserData().get(SequenceNumbers.MAX_SEQ_NO))).isEqualTo(primarySeqNo);
            assertThat(recoveringEngine.getProcessedLocalCheckpoint()).isEqualTo(primarySeqNo);
            assertThat(recoveringEngine.getPersistedLocalCheckpoint()).isEqualTo(primarySeqNo);
            assertThat(recoveringEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(primarySeqNo);
            assertThat(generateNewSeqNo(recoveringEngine)).isEqualTo(primarySeqNo + 1);
        }
    }

    // this test writes documents to the engine while concurrently flushing/commit
    // and ensuring that the commit points contain the correct sequence number data
    @Test
    public void testConcurrentWritesAndCommits() throws Exception {
        List<Engine.IndexCommitRef> commits = new ArrayList<>();
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(defaultSettings, store, createTempDir(), newMergePolicy(), null))) {
            final int numIndexingThreads = scaledRandomIntBetween(2, 4);
            final int numDocsPerThread = randomIntBetween(500, 1000);
            final CyclicBarrier barrier = new CyclicBarrier(numIndexingThreads + 1);
            final List<Thread> indexingThreads = new ArrayList<>();
            final CountDownLatch doneLatch = new CountDownLatch(numIndexingThreads);
            // create N indexing threads to index documents simultaneously
            for (int threadNum = 0; threadNum < numIndexingThreads; threadNum++) {
                final int threadIdx = threadNum;
                Thread indexingThread = new Thread(() -> {
                    try {
                        barrier.await(); // wait for all threads to start at the same time
                        // index random number of docs
                        for (int i = 0; i < numDocsPerThread; i++) {
                            final String id = "thread" + threadIdx + "#" + i;
                            ParsedDocument doc = testParsedDocument(id, testDocument(), B_1);
                            engine.index(indexForDoc(doc));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        doneLatch.countDown();
                    }

                });
                indexingThreads.add(indexingThread);
            }

            // start the indexing threads
            for (Thread thread : indexingThreads) {
                thread.start();
            }
            barrier.await(); // wait for indexing threads to all be ready to start
            int commitLimit = randomIntBetween(10, 20);
            long sleepTime = 1;
            // create random commit points
            boolean doneIndexing;
            do {
                doneIndexing = doneLatch.await(sleepTime, TimeUnit.MILLISECONDS);
                commits.add(engine.acquireLastIndexCommit(true));
                if (commits.size() > commitLimit) { // don't keep on piling up too many commits
                    IOUtils.close(commits.remove(randomIntBetween(0, commits.size()-1)));
                    // we increase the wait time to make sure we eventually if things are slow wait for threads to finish.
                    // this will reduce pressure on disks and will allow threads to make progress without piling up too many commits
                    sleepTime = sleepTime * 2;
                }
            } while (doneIndexing == false);

            // now, verify all the commits have the correct docs according to the user commit data
            long prevLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
            long prevMaxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
            for (Engine.IndexCommitRef commitRef : commits) {
                final IndexCommit commit = commitRef.getIndexCommit();
                Map<String, String> userData = commit.getUserData();
                long localCheckpoint = userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) ?
                    Long.parseLong(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) :
                    SequenceNumbers.NO_OPS_PERFORMED;
                long maxSeqNo = userData.containsKey(SequenceNumbers.MAX_SEQ_NO) ?
                    Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO)) :
                    UNASSIGNED_SEQ_NO;
                // local checkpoint and max seq no shouldn't go backwards
                assertThat(localCheckpoint).isGreaterThanOrEqualTo(prevLocalCheckpoint);
                assertThat(maxSeqNo).isGreaterThanOrEqualTo(prevMaxSeqNo);
                try (IndexReader reader = DirectoryReader.open(commit)) {
                    Long highest = getHighestSeqNo(reader);
                    final long highestSeqNo;
                    if (highest != null) {
                        highestSeqNo = highest.longValue();
                    } else {
                        highestSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                    }
                    // make sure localCheckpoint <= highest seq no found <= maxSeqNo
                    assertThat(highestSeqNo).isGreaterThanOrEqualTo(localCheckpoint);
                    assertThat(highestSeqNo).isLessThanOrEqualTo(maxSeqNo);
                    // make sure all sequence numbers up to and including the local checkpoint are in the index
                    FixedBitSet seqNosBitSet = getSeqNosSet(reader, highestSeqNo);
                    for (int i = 0; i <= localCheckpoint; i++) {
                        assertThat(seqNosBitSet.get(i)).as("local checkpoint [" + localCheckpoint + "], _seq_no [" + i + "] should be indexed").isTrue();
                    }
                }
                prevLocalCheckpoint = localCheckpoint;
                prevMaxSeqNo = maxSeqNo;
            }
        }
    }

    private static Long getHighestSeqNo(final IndexReader reader) throws IOException {
        final String fieldName = SysColumns.Names.SEQ_NO;
        long size = PointValues.size(reader, fieldName);
        if (size == 0) {
            return null;
        }
        byte[] max = PointValues.getMaxPackedValue(reader, fieldName);
        return LongPoint.decodeDimension(max, 0);
    }

    private static FixedBitSet getSeqNosSet(final IndexReader reader, final long highestSeqNo) throws IOException {
        // _seq_no are stored as doc values for the time being, so this is how we get them
        // (as opposed to using an IndexSearcher or IndexReader)
        final FixedBitSet bitSet = new FixedBitSet((int) highestSeqNo + 1);
        final List<LeafReaderContext> leaves = reader.leaves();
        if (leaves.isEmpty()) {
            return bitSet;
        }

        for (int i = 0; i < leaves.size(); i++) {
            final LeafReader leaf = leaves.get(i).reader();
            final NumericDocValues values = leaf.getNumericDocValues(SysColumns.Names.SEQ_NO);
            if (values == null) {
                continue;
            }
            final Bits bits = leaf.getLiveDocs();
            for (int docID = 0; docID < leaf.maxDoc(); docID++) {
                if (bits == null || bits.get(docID)) {
                    if (values.advanceExact(docID) == false) {
                        throw new AssertionError("Document does not have a seq number: " + docID);
                    }
                    final long seqNo = values.longValue();
                    assertThat(bitSet.get((int) seqNo)).as("should not have more than one document with the same seq_no[" +
                                seqNo + "]").isFalse();
                    bitSet.set((int) seqNo);
                }
            }
        }
        return bitSet;
    }

    // #8603: make sure we can separately log IFD's messages
    @Test
    public void testIndexWriterIFDInfoStream() throws IllegalAccessException, IOException {
        LuceneTestCase.assumeFalse("who tests the tester?", LuceneTestCase.VERBOSE);
        MockAppender mockAppender = new MockAppender("testIndexWriterIFDInfoStream");
        mockAppender.start();

        final Logger iwIFDLogger = LogManager.getLogger("org.elasticsearch.index.engine.Engine.IFD");

        Loggers.addAppender(iwIFDLogger, mockAppender);
        Loggers.setLevel(iwIFDLogger, Level.DEBUG);

        try {
            // First, with DEBUG, which should NOT log IndexWriter output:
            ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), B_1);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertThat(mockAppender.sawIndexWriterMessage).isFalse();
            assertThat(mockAppender.sawIndexWriterIFDMessage).isFalse();

            // Again, with TRACE, which should only log IndexWriter IFD output:
            Loggers.setLevel(iwIFDLogger, Level.TRACE);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertThat(mockAppender.sawIndexWriterMessage).isFalse();
            assertThat(mockAppender.sawIndexWriterIFDMessage).isTrue();

        } finally {
            Loggers.removeAppender(iwIFDLogger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(iwIFDLogger, (Level) null);
        }
    }

    @Test
    public void testEnableGcDeletes() throws Exception {
        try (Store store = createStore();
             Engine engine = createEngine(config(defaultSettings, store, createTempDir(), newMergePolicy(), null))) {
            engine.config().setEnableGcDeletes(false);

            final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;

            // Add document
            Document document = testDocument();
            document.add(new TextField("value", "test1", Field.Store.YES));

            ParsedDocument doc = testParsedDocument("1", document, B_2);
            engine.index(new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 0, 1,
                                          VersionType.EXTERNAL,
                                          Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0));

            // Delete document we just added:
            engine.delete(new Engine.Delete(
                "1",
                newUid(doc),
                UNASSIGNED_SEQ_NO,
                0,
                10,
                VersionType.EXTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                UNASSIGNED_SEQ_NO,
                0
            ));

            // Get should not find the document
            Engine.GetResult getResult = engine.get(newGet(doc), searcherFactory);
            assertThat(getResult.docIdAndVersion()).isNull();

            // Give the gc pruning logic a chance to kick in
            Thread.sleep(1000);

            if (randomBoolean()) {
                engine.refresh("test");
            }

            // Delete non-existent document
            engine.delete(new Engine.Delete(
                "2",
                newUid("2"),
                UNASSIGNED_SEQ_NO,
                0,
                10,
                VersionType.EXTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                UNASSIGNED_SEQ_NO,
                0
            ));

            // Get should not find the document (we never indexed uid=2):
            getResult = engine.get(new Engine.Get("2", newUid("2")), searcherFactory);
            assertThat(getResult.docIdAndVersion()).isNull();

            // Try to index uid=1 with a too-old version, should fail:
            Engine.Index index = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 0, 2,
                                                  VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
            Engine.IndexResult indexResult = engine.index(index);
            assertThat(indexResult.getResultType()).isEqualTo(Engine.Result.Type.FAILURE);
            assertThat(indexResult.getFailure()).isExactlyInstanceOf(VersionConflictEngineException.class);

            // Get should still not find the document
            getResult = engine.get(newGet(doc), searcherFactory);
            assertThat(getResult.docIdAndVersion()).isNull();

            // Try to index uid=2 with a too-old version, should fail:
            Engine.Index index1 = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 0, 2,
                                                   VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
            indexResult = engine.index(index1);
            assertThat(indexResult.getResultType()).isEqualTo(Engine.Result.Type.FAILURE);
            assertThat(indexResult.getFailure()).isExactlyInstanceOf(VersionConflictEngineException.class);

            // Get should not find the document
            getResult = engine.get(newGet(doc), searcherFactory);
            assertThat(getResult.docIdAndVersion()).isNull();
        }
    }

    @Test
    public void testExtractShardId() {
        try (Engine.Searcher test = this.engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            ShardId shardId = ShardUtils.extractShardId(test.getDirectoryReader());
            assertThat(shardId).isNotNull();
            assertThat(engine.config().getShardId()).isEqualTo(shardId);
        }
    }

    /**
     * Random test that throws random exception and ensures all references are
     * counted down / released and resources are closed.
     */
    @Test
    public void testFailStart() throws IOException {
        // this test fails if any reader, searcher or directory is not closed - MDW FTW
        final int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            MockDirectoryWrapper wrapper = newMockDirectory();
            wrapper.setFailOnOpenInput(randomBoolean());
            wrapper.setAllowRandomFileNotFoundException(randomBoolean());
            wrapper.setRandomIOExceptionRate(randomDouble());
            wrapper.setRandomIOExceptionRateOnOpen(randomDouble());
            final Path translogPath = createTempDir("testFailStart");
            try (Store store = createStore(wrapper)) {
                int refCount = store.refCount();
                assertThat(store.refCount() > 0).as("refCount: " + store.refCount()).isTrue();
                InternalEngine holder;
                try {
                    holder = createEngine(store, translogPath);
                } catch (EngineCreationFailureException | IOException ex) {
                    assertThat(refCount).isEqualTo(store.refCount());
                    continue;
                }
                assertThat(refCount + 1).isEqualTo(store.refCount());
                final int numStarts = scaledRandomIntBetween(1, 5);
                for (int j = 0; j < numStarts; j++) {
                    try {
                        assertThat(refCount + 1).isEqualTo(store.refCount());
                        holder.close();
                        holder = createEngine(store, translogPath);
                        assertThat(refCount + 1).isEqualTo(store.refCount());
                    } catch (EngineCreationFailureException ex) {
                        // all is fine
                        assertThat(refCount).isEqualTo(store.refCount());
                        break;
                    }
                }
                holder.close();
                assertThat(refCount).isEqualTo(store.refCount());
            }
        }
    }

    @Test
    public void testSettings() {
        CodecService codecService = new CodecService();
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();

        assertThat(codecService.codec(codecName).getName()).isEqualTo(engine.config().getCodec().getName());
        assertThat(codecService.codec(codecName).getName()).isEqualTo(currentIndexWriterConfig.getCodec().getName());
    }

    @Test
    public void testCurrentTranslogIDisCommitted() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null,
                                         globalCheckpoint::get);

            // create
            {
                store.createEmpty(Version.CURRENT.luceneVersion);
                final String translogUUID =
                    Translog.createEmptyTranslog(config.getTranslogConfig().getTranslogPath(),
                                                 SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
                store.associateIndexWithNewTranslog(translogUUID);
                ParsedDocument doc = testParsedDocument(Integer.toString(0), testDocument(),
                                                        new BytesArray("{}"));
                Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 0,
                                                                  Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);

                try (InternalEngine engine = createEngine(config)) {
                    engine.index(firstIndexRequest);
                    engine.syncTranslog(); // to advance persisted local checkpoint
                    assertThat(engine.getPersistedLocalCheckpoint()).isEqualTo(engine.getProcessedLocalCheckpoint());
                    globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                    assertThatThrownBy(() -> engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE))
                        .isExactlyInstanceOf(IllegalStateException.class);
                    Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                    assertThat(userData.get(Translog.TRANSLOG_UUID_KEY)).isEqualTo(engine.getTranslog().getTranslogUUID());
                }
            }
            // open and recover tlog
            {
                for (int i = 0; i < 2; i++) {
                    try (InternalEngine engine = new InternalEngine(config)) {
                        assertThatThrownBy(engine::ensureCanFlush)
                            .isExactlyInstanceOf(IllegalStateException.class);
                        Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertThat(userData.get(Translog.TRANSLOG_UUID_KEY)).isEqualTo(engine.getTranslog().getTranslogUUID());
                        engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                        userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertThat(userData.get(Translog.TRANSLOG_UUID_KEY)).isEqualTo(engine.getTranslog().getTranslogUUID());
                    }
                }
            }
            // open index with new tlog
            {
                final String translogUUID =
                    Translog.createEmptyTranslog(config.getTranslogConfig().getTranslogPath(),
                                                 SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
                store.associateIndexWithNewTranslog(translogUUID);
                try (InternalEngine engine = new InternalEngine(config)) {
                    Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                    assertThat(userData.get(Translog.TRANSLOG_UUID_KEY)).isEqualTo(engine.getTranslog().getTranslogUUID());
                    engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                    assertThat(engine.getTranslog().currentFileGeneration()).isEqualTo(2);
                }
            }

            // open and recover tlog with empty tlog
            {
                for (int i = 0; i < 2; i++) {
                    try (InternalEngine engine = new InternalEngine(config)) {
                        Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertThat(userData.get(Translog.TRANSLOG_UUID_KEY)).isEqualTo(engine.getTranslog().getTranslogUUID());
                        engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                        userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertThat(userData.get(Translog.TRANSLOG_UUID_KEY)).isEqualTo(engine.getTranslog().getTranslogUUID());
                    }
                }
            }
        }
    }

    @Test
    public void testMissingTranslog() throws IOException {
        // test that we can force start the engine , even if the translog is missing.
        engine.close();
        // fake a new translog, causing the engine to point to a missing one.
        final long newPrimaryTerm = randomLongBetween(0L, primaryTerm.get());
        final Translog translog = createTranslog(() -> newPrimaryTerm);
        long id = translog.currentFileGeneration();
        translog.close();
        IOUtils.rm(translog.location().resolve(Translog.getFilename(id)));
        assertThatThrownBy(() -> createEngine(store, primaryTranslogDir))
            .as("engine shouldn't start without a valid translog id")
            .isExactlyInstanceOf(EngineCreationFailureException.class);
        // when a new translog is created it should be ok
        final String translogUUID = Translog.createEmptyTranslog(primaryTranslogDir, UNASSIGNED_SEQ_NO, shardId, newPrimaryTerm);
        store.associateIndexWithNewTranslog(translogUUID);
        EngineConfig config = config(defaultSettings, store, primaryTranslogDir, newMergePolicy(), null);
        engine = new InternalEngine(config);
    }

    @Test
    public void testTranslogReplayWithFailure() throws IOException {
        final MockDirectoryWrapper directory = newMockDirectory();
        final Path translogPath = createTempDir("testTranslogReplayWithFailure");
        try (Store store = createStore(directory)) {
            final int numDocs = randomIntBetween(1, 10);
            try (InternalEngine engine = createEngine(store, translogPath)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), new BytesArray("{}"));
                    Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 0,
                                                                      Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
                    Engine.IndexResult indexResult = engine.index(firstIndexRequest);
                    assertThat(indexResult.getVersion()).isEqualTo(1L);
                }
                assertVisibleCount(engine, numDocs);
            }
            // since we rollback the IW we are writing the same segment files again after starting IW but MDW prevents
            // this so we have to disable the check explicitly
            final int numIters = randomIntBetween(3, 5);
            for (int i = 0; i < numIters; i++) {
                directory.setRandomIOExceptionRateOnOpen(randomDouble());
                directory.setRandomIOExceptionRate(randomDouble());
                directory.setFailOnOpenInput(randomBoolean());
                directory.setAllowRandomFileNotFoundException(randomBoolean());
                boolean started = false;
                InternalEngine engine = null;
                try {
                    engine = createEngine(store, translogPath);
                    started = true;
                } catch (EngineException | IOException e) {
                    logger.trace("exception on open", e);
                }
                directory.setRandomIOExceptionRateOnOpen(0.0);
                directory.setRandomIOExceptionRate(0.0);
                directory.setFailOnOpenInput(false);
                directory.setAllowRandomFileNotFoundException(false);
                if (started) {
                    engine.refresh("warm_up");
                    assertVisibleCount(engine, numDocs, false);
                    engine.close();
                }
            }
        }
    }

    @Test
    public void testTranslogCleanUpPostCommitCrash() throws Exception {
        IndexSettings indexSettings = new IndexSettings(defaultSettings.getIndexMetadata(), defaultSettings.getNodeSettings(),
                                                        defaultSettings.getScopedSettings());
        IndexMetadata.Builder builder = IndexMetadata.builder(indexSettings.getIndexMetadata());
        builder.settings(Settings.builder().put(indexSettings.getSettings())
                             .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1")
                             .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1")
        );
        indexSettings.updateIndexMetadata(builder.build());

        try (Store store = createStore()) {
            AtomicBoolean throwErrorOnCommit = new AtomicBoolean();
            final Path translogPath = createTempDir();
            final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            final LongSupplier globalCheckpointSupplier = globalCheckpoint::get;
            store.createEmpty(Version.CURRENT.luceneVersion);
            final String translogUUID = Translog.createEmptyTranslog(translogPath, globalCheckpoint.get(), shardId, primaryTerm.get());
            store.associateIndexWithNewTranslog(translogUUID);
            try (InternalEngine engine =
                     new InternalEngine(config(indexSettings, store, translogPath, newMergePolicy(), null, null,
                                               globalCheckpointSupplier)) {

                         @Override
                         protected void commitIndexWriter(IndexWriter writer, Translog translog, String syncId) throws IOException {
                             super.commitIndexWriter(writer, translog, syncId);
                             if (throwErrorOnCommit.get()) {
                                 throw new RuntimeException("power's out");
                             }
                         }
                     }) {
                engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                final ParsedDocument doc1 = testParsedDocument("1", testDocumentWithTextField(), SOURCE);
                engine.index(indexForDoc(doc1));
                engine.syncTranslog(); // to advance local checkpoint
                assertThat(engine.getPersistedLocalCheckpoint()).isEqualTo(engine.getProcessedLocalCheckpoint());
                globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                throwErrorOnCommit.set(true);
                assertThatThrownBy(engine::flush)
                    .isExactlyInstanceOf(FlushFailedEngineException.class)
                    .hasMessage("Flush failed")
                    .cause()
                        .hasMessage("power's out");
            }
            try (InternalEngine engine =
                     new InternalEngine(config(indexSettings, store, translogPath, newMergePolicy(), null, null,
                                               globalCheckpointSupplier))) {
                engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                assertVisibleCount(engine, 1);
                final long localCheckpoint = Long.parseLong(
                    engine.getLastCommittedSegmentInfos().userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
                final long committedGen = engine.getTranslog().getMinGenerationForSeqNo(localCheckpoint + 1).translogFileGeneration;
                for (int gen = 1; gen < committedGen; gen++) {
                    final Path genFile = translogPath.resolve(Translog.getFilename(gen));
                    assertThat(Files.exists(genFile)).as(genFile + " wasn't cleaned up").isFalse();
                }
            }
        }
    }

    @Test
    public void testSkipTranslogReplay() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), new BytesArray("{}"));
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 0,
                                                              Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion()).isEqualTo(1L);
        }
        EngineConfig config = engine.config();
        assertVisibleCount(engine, numDocs);
        engine.close();
        try (InternalEngine engine = new InternalEngine(config)) {
            engine.skipTranslogRecovery();
            try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
                assertThat(topDocs.totalHits.value).isEqualTo(0L);
            }
        }
    }

    @Test
    public void testTranslogReplay() throws IOException {
        final LongSupplier inSyncGlobalCheckpointSupplier = () -> this.engine.getProcessedLocalCheckpoint();
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), new BytesArray("{}"));
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 1,
                                                              Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion()).isEqualTo(1L);
        }
        assertVisibleCount(engine, numDocs);
        translogHandler = createTranslogHandler(engine.engineConfig.getIndexSettings());

        engine.close();
        // we need to reuse the engine config unless the parser.mappingModified won't work
        engine = new InternalEngine(copy(engine.config(), inSyncGlobalCheckpointSupplier));
        engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        engine.refresh("warm_up");

        assertVisibleCount(engine, numDocs, false);

        engine.close();
        translogHandler = createTranslogHandler(engine.engineConfig.getIndexSettings());
        engine = createEngine(store, primaryTranslogDir, inSyncGlobalCheckpointSupplier);
        engine.refresh("warm_up");
        assertVisibleCount(engine, numDocs, false);

        final boolean flush = randomBoolean();
        int randomId = randomIntBetween(numDocs + 1, numDocs + 10);
        ParsedDocument doc = testParsedDocument(Integer.toString(randomId), testDocument(), new BytesArray("{}"));
        Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 1, 1,
                                                          VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult indexResult = engine.index(firstIndexRequest);
        assertThat(indexResult.getVersion()).isEqualTo(1L);
        if (flush) {
            engine.flush();
            engine.refresh("test");
        }

        doc = testParsedDocument(Integer.toString(randomId), testDocument(), new BytesArray("{}"));
        Engine.Index idxRequest = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 1, 2,
                                                   VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult result = engine.index(idxRequest);
        engine.refresh("test");
        assertThat(result.getVersion()).isEqualTo(2L);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits.value).isEqualTo(numDocs + 1L);
        }

        engine.close();
        translogHandler = createTranslogHandler(engine.engineConfig.getIndexSettings());
        engine = createEngine(store, primaryTranslogDir, inSyncGlobalCheckpointSupplier);
        engine.refresh("warm_up");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits.value).isEqualTo(numDocs + 1L);
        }
        engine.delete(new Engine.Delete(
            Integer.toString(randomId),
            newUid(doc),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            UNASSIGNED_SEQ_NO,
            0
        ));
        if (randomBoolean()) {
            engine.close();
            engine = createEngine(store, primaryTranslogDir, inSyncGlobalCheckpointSupplier);
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), numDocs);
            assertThat(topDocs.totalHits.value).isEqualTo((long) numDocs);
        }
    }

    @Test
    public void testRecoverFromForeignTranslog() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), testDocument(), new BytesArray("{}"));
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 1,
                                                              Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
            Engine.IndexResult index = engine.index(firstIndexRequest);
            assertThat(index.getVersion()).isEqualTo(1L);
        }
        assertVisibleCount(engine, numDocs);
        Translog.TranslogGeneration generation = engine.getTranslog().getGeneration();
        engine.close();

        final Path badTranslogLog = createTempDir();
        final String badUUID = Translog.createEmptyTranslog(badTranslogLog, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        Translog translog = new Translog(
            new TranslogConfig(shardId, badTranslogLog, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE),
            badUUID, createTranslogDeletionPolicy(INDEX_SETTINGS), () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTerm::get, seqNo -> {});
        translog.add(new Translog.Index(
            "SomeBogusId",
            0,
            primaryTerm.get(),
            "{}".getBytes(Charset.forName("UTF-8"))));
        assertThat(translog.currentFileGeneration()).isEqualTo(generation.translogFileGeneration);
        translog.close();

        EngineConfig config = engine.config();
        /* create a TranslogConfig that has been created with a different UUID */
        TranslogConfig translogConfig = new TranslogConfig(shardId, translog.location(), config.getIndexSettings(),
                                                           BigArrays.NON_RECYCLING_INSTANCE);

        EngineConfig brokenConfig = new EngineConfig(
            shardId,
            threadPool,
            config.getIndexSettings(),
            store,
            newMergePolicy(),
            config.getAnalyzer(),
            new CodecService(),
            config.getEventListener(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            translogConfig,
            TimeValue.timeValueMinutes(5),
            config.getExternalRefreshListener(),
            config.getInternalRefreshListener(),
            new NoneCircuitBreakerService(),
            () -> UNASSIGNED_SEQ_NO,
            () -> RetentionLeases.EMPTY,
            primaryTerm::get,
            tombstoneDocSupplier()
        );
        assertThatThrownBy(() -> new InternalEngine(brokenConfig))
            .isExactlyInstanceOf(EngineCreationFailureException.class);

        engine = createEngine(store, primaryTranslogDir); // and recover again!
        assertVisibleCount(engine, numDocs, true);
    }

    @Test
    public void testShardNotAvailableExceptionWhenEngineClosedConcurrently() throws IOException, InterruptedException {
        AtomicReference<Exception> exception = new AtomicReference<>();
        String operation = randomFrom("optimize", "refresh", "flush");
        Thread mergeThread = new Thread() {
            @Override
            public void run() {
                boolean stop = false;
                logger.info("try with {}", operation);
                while (stop == false) {
                    try {
                        switch (operation) {
                            case "optimize": {
                                engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
                                break;
                            }
                            case "refresh": {
                                engine.refresh("test refresh");
                                break;
                            }
                            case "flush": {
                                engine.flush(true, true);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        exception.set(e);
                        stop = true;
                    }
                }
            }
        };
        mergeThread.start();
        engine.close();
        mergeThread.join();
        logger.info("exception caught: ", exception.get());
        assertThat(TransportActions.isShardNotAvailableException(exception.get())).as("expected an Exception that signals shard is not available").isTrue();
    }

    /**
     * Tests that when the close method returns the engine is actually guaranteed to have cleaned up and that resources are closed
     */
    @Test
    public void testConcurrentEngineClosed() throws BrokenBarrierException, InterruptedException {
        Thread[] closingThreads = new Thread[3];
        CyclicBarrier barrier = new CyclicBarrier(1 + closingThreads.length + 1);
        Thread failEngine = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            protected void doRun() throws Exception {
                barrier.await();
                engine.failEngine("test", new RuntimeException("test"));
            }
        });
        failEngine.start();
        for (int i = 0;i < closingThreads.length ; i++) {
            boolean flushAndClose = randomBoolean();
            closingThreads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    if (flushAndClose) {
                        engine.flushAndClose();
                    } else {
                        engine.close();
                    }
                    // try to acquire the writer lock - i.e., everything is closed, we need to synchronize
                    // to avoid races between closing threads
                    synchronized (closingThreads) {
                        try (Lock ignored = store.directory().obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                            // all good.
                        }
                    }
                }
            });
            closingThreads[i].setName("closingThread_" + i);
            closingThreads[i].start();
        }
        barrier.await();
        failEngine.join();
        for (Thread t : closingThreads) {
            t.join();
        }
    }

    private static class ThrowingIndexWriter extends IndexWriter {
        private AtomicReference<Supplier<Exception>> failureToThrow = new AtomicReference<>();

        ThrowingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }

        @Override
        public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
            maybeThrowFailure();
            return super.addDocument(doc);
        }

        private void maybeThrowFailure() throws IOException {
            if (failureToThrow.get() != null) {
                Exception failure = failureToThrow.get().get();
                clearFailure(); // one shot
                if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                } else if (failure instanceof IOException) {
                    throw (IOException) failure;
                } else {
                    assert false: "unsupported failure class: " + failure.getClass().getCanonicalName();
                }
            }
        }

        @Override
        public long softUpdateDocument(Term term, Iterable<? extends IndexableField> doc, Field... softDeletes) throws IOException {
            maybeThrowFailure();
            return super.softUpdateDocument(term, doc, softDeletes);
        }

        @Override
        public long deleteDocuments(Term... terms) throws IOException {
            maybeThrowFailure();
            return super.deleteDocuments(terms);
        }

        public void setThrowFailure(Supplier<Exception> failureSupplier) {
            failureToThrow.set(failureSupplier);
        }

        public void clearFailure() {
            failureToThrow.set(null);
        }
    }

    @Test
    public void testHandleDocumentFailure() throws Exception {
        try (Store store = createStore()) {
            final ParsedDocument doc1 = testParsedDocument("1", testDocumentWithTextField(), B_1);
            final ParsedDocument doc2 = testParsedDocument("2", testDocumentWithTextField(), B_1);
            final ParsedDocument doc3 = testParsedDocument("3", testDocumentWithTextField(), B_1);

            AtomicReference<ThrowingIndexWriter> throwingIndexWriter = new AtomicReference<>();
            try (InternalEngine engine = createEngine(
                defaultSettings,
                store,
                createTempDir(),
                NoMergePolicy.INSTANCE,
                (directory, iwc) -> {
                    throwingIndexWriter.set(new ThrowingIndexWriter(directory, iwc));
                    return throwingIndexWriter.get();
                })
            ) {
                // test document failure while indexing
                if (randomBoolean()) {
                    throwingIndexWriter.get().setThrowFailure(() -> new IOException("simulated"));
                } else {
                    throwingIndexWriter.get().setThrowFailure(() -> new IllegalArgumentException("simulated max token length"));
                }
                // test index with document failure
                Engine.IndexResult indexResult = engine.index(indexForDoc(doc1));
                assertThat(indexResult.getFailure()).isNotNull();
                assertThat(indexResult.getSeqNo()).isEqualTo(0L);
                assertThat(indexResult.getVersion()).isEqualTo(Versions.MATCH_ANY);
                assertThat(indexResult.getTranslogLocation()).isNotNull();

                throwingIndexWriter.get().clearFailure();
                indexResult = engine.index(indexForDoc(doc1));
                assertThat(indexResult.getSeqNo()).isEqualTo(1L);
                assertThat(indexResult.getVersion()).isEqualTo(1L);
                assertThat(indexResult.getFailure()).isNull();
                assertThat(indexResult.getTranslogLocation()).isNotNull();
                engine.index(indexForDoc(doc2));

                engine.close();

                // now the engine is closed check we respond correctly
                assertThatThrownBy(() -> engine.index(indexForDoc(doc1))).isExactlyInstanceOf(AlreadyClosedException.class);
                assertThatThrownBy(() -> engine.delete(new Engine.Delete(
                                 "1",
                                 newUid(doc1),
                                 UNASSIGNED_SEQ_NO,
                                 primaryTerm.get(),
                                 Versions.MATCH_ANY,
                                 VersionType.INTERNAL,
                                 Engine.Operation.Origin.PRIMARY,
                                 System.nanoTime(),
                                 UNASSIGNED_SEQ_NO,
                                 0))).isExactlyInstanceOf(AlreadyClosedException.class);
                assertThatThrownBy(() -> engine.noOp(
                                 new Engine.NoOp(engine.getLocalCheckpointTracker().generateSeqNo(),
                                                 engine.config().getPrimaryTermSupplier().getAsLong(),
                                                 randomFrom(Engine.Operation.Origin.values()),
                                                 randomNonNegativeLong(),
                                                 "test"))).isExactlyInstanceOf(AlreadyClosedException.class);
            }
        }
    }

    @Test
    public void testDeleteWithFatalError() throws Exception {
        final IllegalStateException tragicException = new IllegalStateException("fail to store tombstone");
        try (Store store = createStore()) {
            EngineConfig.TombstoneDocSupplier tombstoneDocSupplier = new EngineConfig.TombstoneDocSupplier() {
                @Override
                public ParsedDocument newDeleteTombstoneDoc(String id) {
                    ParsedDocument parsedDocument = tombstoneDocSupplier().newDeleteTombstoneDoc(id);
                    parsedDocument.doc().add(new StoredField("foo", "bar") {
                        // this is a hack to add a failure during store document which triggers a tragic event
                        // and in turn fails the engine
                        @Override
                        public StoredValue storedValue() {
                            throw tragicException;
                        }
                    });
                    return parsedDocument;
                }

                @Override
                public ParsedDocument newNoopTombstoneDoc(String reason) {
                    return tombstoneDocSupplier().newNoopTombstoneDoc(reason);
                }
            };
            EngineConfig config = config(this.engine.config(), store, createTempDir(), tombstoneDocSupplier);
            try (InternalEngine engine = createEngine(config)) {
                final ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), SOURCE);
                engine.index(indexForDoc(doc));
                assertThatThrownBy(() -> engine.delete(
                    new Engine.Delete(
                        "1",
                        newUid(doc),
                        UNASSIGNED_SEQ_NO,
                        primaryTerm.get(),
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(),
                        UNASSIGNED_SEQ_NO,
                        0
                    ))).isExactlyInstanceOf(IllegalStateException.class);
                assertThat(engine.isClosed.get()).isTrue();
                assertThat(engine.failedEngine.get()).isSameAs(tragicException);
            }
        }
    }

    @Test
    public void testDoubleDeliveryPrimary() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(),
            new BytesArray("{}".getBytes(Charset.defaultCharset())));
        final boolean create = randomBoolean();
        Engine.Index operation = appendOnlyPrimary(doc, false, 1, create);
        Engine.Index retry = appendOnlyPrimary(doc, true, 1, create);
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertLuceneOperations(engine, 1, 0, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(1L));
            assertThat(indexResult.getTranslogLocation()).isNotNull();
            Engine.IndexResult retryResult = engine.index(retry);
            assertLuceneOperations(engine, 1, create ? 0 : 1, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(1L));
            if (create) {
                assertThat(retryResult.getTranslogLocation()).isNull();
            } else {
                assertThat(retryResult.getTranslogLocation()).isNotNull();
            }
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertLuceneOperations(engine, 1, 0, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(1L));
            assertThat(retryResult.getTranslogLocation()).isNotNull();
            Engine.IndexResult indexResult = engine.index(operation);
            assertLuceneOperations(engine, 1, create ? 0 : 1, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(2L));
            assertThat(retryResult.getTranslogLocation()).isNotNull();
            if (create) {
                assertThat(indexResult.getTranslogLocation()).isNull();
            } else {
                assertThat(indexResult.getTranslogLocation()).isNotNull();
            }
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }
        operation = appendOnlyPrimary(doc, false, 1, create);
        retry = appendOnlyPrimary(doc, true, 1, create);
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            if (create) {
                assertThat(indexResult.getTranslogLocation()).isNull();
            } else {
                assertThat(indexResult.getTranslogLocation()).isNotNull();
            }
            Engine.IndexResult retryResult = engine.index(retry);
            if (create) {
                assertThat(retryResult.getTranslogLocation()).isNull();
            } else {
                assertThat(retryResult.getTranslogLocation()).isNotNull();
            }
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            if (create) {
                assertThat(retryResult.getTranslogLocation()).isNull();
            } else {
                assertThat(retryResult.getTranslogLocation()).isNotNull();
            }
            Engine.IndexResult indexResult = engine.index(operation);
            if (create) {
                assertThat(indexResult.getTranslogLocation()).isNull();
            } else {
                assertThat(indexResult.getTranslogLocation()).isNotNull();
            }
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }
    }

    @Test
    public void testDoubleDeliveryReplicaAppendingAndDeleteOnly() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(),
                                                      new BytesArray("{}".getBytes(Charset.defaultCharset())));
        Engine.Index operation = appendOnlyReplica(doc, false, 1, randomIntBetween(0, 5));
        Engine.Index retry = appendOnlyReplica(doc, true, 1, randomIntBetween(0, 5));
        Engine.Delete delete = new Engine.Delete(
            operation.id(),
            operation.uid(),
            Math.max(retry.seqNo(), operation.seqNo()) + 1,
            operation.primaryTerm(),
            operation.version() + 1,
            operation.versionType(),
            REPLICA,
            operation.startTime() + 1,
            UNASSIGNED_SEQ_NO,
            0
        );
        // operations with a seq# equal or lower to the local checkpoint are not indexed to lucene
        // and the version lookup is skipped
        final boolean sameSeqNo = operation.seqNo() == retry.seqNo();
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertLuceneOperations(engine, 1, 0, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(0L));
            assertThat(indexResult.getTranslogLocation()).isNotNull();
            engine.delete(delete);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(1L));
            assertLuceneOperations(engine, 1, 0, 1);
            Engine.IndexResult retryResult = engine.index(retry);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(sameSeqNo ? 1L : 2L));
            assertThat(retryResult.getTranslogLocation()).isNotNull();
            assertThat(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0).isTrue();
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertLuceneOperations(engine, 1, 0, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(0L));
            assertThat(retryResult.getTranslogLocation()).isNotNull();
            engine.delete(delete);
            assertLuceneOperations(engine, 1, 0, 1);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(1L));
            Engine.IndexResult indexResult = engine.index(operation);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(sameSeqNo ? 1L : 2L));
            assertThat(retryResult.getTranslogLocation()).isNotNull();
            assertThat(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0).isTrue();
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(0);
        }
    }

    @Test
    public void testDoubleDeliveryReplicaAppendingOnly() throws IOException {
        final Supplier<ParsedDocument> doc = () -> testParsedDocument("1", testDocumentWithTextField(),
            new BytesArray("{}".getBytes(Charset.defaultCharset())));
        boolean replicaOperationIsRetry = randomBoolean();
        Engine.Index operation = appendOnlyReplica(doc.get(), replicaOperationIsRetry, 1, randomIntBetween(0, 5));

        Engine.IndexResult result = engine.index(operation);
        assertLuceneOperations(engine, 1, 0, 0);
        assertThat(engine.getNumVersionLookups()).isEqualTo(0);
        assertThat(result.getTranslogLocation()).isNotNull();

        // promote to primary: first do refresh
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }

        final boolean create = randomBoolean();
        operation = appendOnlyPrimary(doc.get(), false, 1, create);
        Engine.Index retry = appendOnlyPrimary(doc.get(), true, 1, create);
        if (randomBoolean()) {
            // if the replica operation wasn't a retry, the operation arriving on the newly promoted primary must be a retry
            if (replicaOperationIsRetry) {
                Engine.IndexResult indexResult = engine.index(operation);
                if (create) {
                    assertThat(indexResult.getTranslogLocation()).isNull();
                } else {
                    assertThat(indexResult.getTranslogLocation()).isNotNull();
                }
            }
            Engine.IndexResult retryResult = engine.index(retry);
            if (create) {
                assertThat(retryResult.getTranslogLocation()).isNull();
            } else {
                assertThat(retryResult.getTranslogLocation()).isNotNull();
            }
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            if (create) {
                assertThat(retryResult.getTranslogLocation()).isNull();
            } else {
                assertThat(retryResult.getTranslogLocation()).isNotNull();
            }
            Engine.IndexResult indexResult = engine.index(operation);
            if (create) {
                assertThat(indexResult.getTranslogLocation()).isNull();
            } else {
                assertThat(indexResult.getTranslogLocation()).isNotNull();
            }
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }
    }

    @Test
    public void testDoubleDeliveryReplica() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(),
                                                      new BytesArray("{}".getBytes(Charset.defaultCharset())));
        Engine.Index operation = replicaIndexForDoc(doc, 1, 20, false);
        Engine.Index duplicate = replicaIndexForDoc(doc, 1, 20, true);
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertLuceneOperations(engine, 1, 0, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(0L));
            assertThat(indexResult.getTranslogLocation()).isNotNull();
            if (randomBoolean()) {
                engine.refresh("test");
            }
            Engine.IndexResult retryResult = engine.index(duplicate);
            assertLuceneOperations(engine, 1, 0, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(0L));
            assertThat(retryResult.getTranslogLocation()).isNotNull();
            assertThat(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0).isTrue();
        } else {
            Engine.IndexResult retryResult = engine.index(duplicate);
            assertLuceneOperations(engine, 1, 0, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(0L));
            assertThat(retryResult.getTranslogLocation()).isNotNull();
            if (randomBoolean()) {
                engine.refresh("test");
            }
            Engine.IndexResult indexResult = engine.index(operation);
            assertLuceneOperations(engine, 1, 0, 0);
            assertThatIfAssertionEnabled(engine.getNumVersionLookups(), n -> assertThat(n).isEqualTo(0L));
            assertThat(retryResult.getTranslogLocation()).isNotNull();
            assertThat(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0).isTrue();
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }
        if (engine.engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            List<Translog.Operation> ops = readAllOperationsInLucene(engine);
            assertThat(ops.stream().map(o -> o.seqNo()).toList()).contains(20L);
        }
    }

    @Test
    public void testRetryWithAutogeneratedIdWorksAndNoDuplicateDocs() throws IOException {

        final ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(),
                                                      new BytesArray("{}".getBytes(Charset.defaultCharset())));
        boolean isRetry = false;
        long autoGeneratedIdTimestamp = 0;

        Engine.Index index = new Engine.Index(
            newUid(doc),
            doc,
            UNASSIGNED_SEQ_NO,
            1,
            randomBoolean() ? Versions.MATCH_DELETED : Versions.MATCH_ANY,
            VersionType.INTERNAL,
            PRIMARY,
            System.nanoTime(),
            autoGeneratedIdTimestamp,
            isRetry,
            UNASSIGNED_SEQ_NO,
            0
        );
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion()).isEqualTo(1L);

        index = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), index.primaryTerm(), indexResult.getVersion(),
                                 null, REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry, UNASSIGNED_SEQ_NO, 0);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion()).isEqualTo(1L);

        isRetry = true;
        index = new Engine.Index(
            newUid(doc),
            doc,
            UNASSIGNED_SEQ_NO,
            1,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            PRIMARY,
            System.nanoTime(),
            autoGeneratedIdTimestamp,
            isRetry,
            UNASSIGNED_SEQ_NO,
            0
        );
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion()).isEqualTo(1L);
        assertThat(indexResult.getSeqNo()).isNotEqualTo(UNASSIGNED_SEQ_NO);
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }

        index = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), index.primaryTerm(), indexResult.getVersion(),
                                 null, REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry, UNASSIGNED_SEQ_NO, 0);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getResultType()).isEqualTo(Engine.Result.Type.SUCCESS);
        replicaEngine.refresh("test");
        try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }
    }

    @Test
    public void testRetryWithAutogeneratedIdsAndWrongOrderWorksAndNoDuplicateDocs() throws IOException {

        final ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(),
                                                      new BytesArray("{}".getBytes(Charset.defaultCharset())));
        boolean isRetry = true;
        long autoGeneratedIdTimestamp = 0;

        Engine.Index firstIndexRequest = new Engine.Index(
            newUid(doc),
            doc,
            UNASSIGNED_SEQ_NO,
            1,
            randomBoolean() ? Versions.MATCH_DELETED : Versions.MATCH_ANY,
            VersionType.INTERNAL,
            PRIMARY,
            System.nanoTime(),
            autoGeneratedIdTimestamp,
            isRetry,
            UNASSIGNED_SEQ_NO,
            0
        );
        Engine.IndexResult result = engine.index(firstIndexRequest);
        assertThat(result.getVersion()).isEqualTo(1L);

        Engine.Index firstIndexRequestReplica = new Engine.Index(newUid(doc), doc, result.getSeqNo(), firstIndexRequest.primaryTerm(),
                                                                 result.getVersion(), null, REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry, UNASSIGNED_SEQ_NO, 0);
        Engine.IndexResult indexReplicaResult = replicaEngine.index(firstIndexRequestReplica);
        assertThat(indexReplicaResult.getVersion()).isEqualTo(1L);

        isRetry = false;
        Engine.Index secondIndexRequest = new Engine.Index(
            newUid(doc),
            doc,
            UNASSIGNED_SEQ_NO,
            1,
            Versions.MATCH_DELETED,
            VersionType.INTERNAL,
            PRIMARY,
            System.nanoTime(),
            autoGeneratedIdTimestamp,
            isRetry,
            UNASSIGNED_SEQ_NO,
            0);
        Engine.IndexResult indexResult = engine.index(secondIndexRequest);
        assertThat(indexResult.isCreated()).isFalse();
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }

        Engine.Index secondIndexRequestReplica = new Engine.Index(newUid(doc), doc, result.getSeqNo(), secondIndexRequest.primaryTerm(),
                                                                  result.getVersion(), null, REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry, UNASSIGNED_SEQ_NO, 0);
        replicaEngine.index(secondIndexRequestReplica);
        replicaEngine.refresh("test");
        try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(topDocs.totalHits.value).isEqualTo(1);
        }
    }

    public Engine.Index randomAppendOnly(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp) {
        if (randomBoolean()) {
            return appendOnlyPrimary(doc, retry, autoGeneratedIdTimestamp);
        } else {
            return appendOnlyReplica(doc, retry, autoGeneratedIdTimestamp, 0);
        }
    }

    public Engine.Index appendOnlyPrimary(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp, boolean create) {
        return new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 1, create ? Versions.MATCH_DELETED : Versions.MATCH_ANY,
            VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, retry,
            UNASSIGNED_SEQ_NO, 0);
    }

    public Engine.Index appendOnlyPrimary(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp) {
        return appendOnlyPrimary(doc, retry, autoGeneratedIdTimestamp, randomBoolean());
    }

    public Engine.Index appendOnlyReplica(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp, final long seqNo) {
        return new Engine.Index(newUid(doc), doc, seqNo, 2, 1, null,
                                Engine.Operation.Origin.REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, retry, UNASSIGNED_SEQ_NO, 0);
    }


    @Test
    public void testAppendConcurrently() throws InterruptedException, IOException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        int numDocs = randomIntBetween(1000, 10000);
        assertThat(engine.getNumVersionLookups()).isEqualTo(0);
        assertThat(engine.getNumIndexVersionsLookups()).isEqualTo(0);
        boolean primary = randomBoolean();
        List<Engine.Index> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(i),
                testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())));
            Engine.Index index = primary ? appendOnlyPrimary(doc, false, i) : appendOnlyReplica(doc, false, i, i);
            docs.add(index);
        }
        Collections.shuffle(docs, random());
        CountDownLatch startGun = new CountDownLatch(thread.length);

        AtomicInteger offset = new AtomicInteger(-1);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    startGun.countDown();
                    try {
                        startGun.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    assertThat(engine.getVersionMap().values()).isEmpty();
                    int docOffset;
                    while ((docOffset = offset.incrementAndGet()) < docs.size()) {
                        try {
                            engine.index(docs.get(docOffset));
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            };
            thread[i].start();
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertThat(searcher.getIndexReader().maxDoc()).as("unexpected refresh").isEqualTo(0);
        }
        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            int count = searcher.count(new MatchAllDocsQuery());
            assertThat(count).isEqualTo(docs.size());
        }
        assertThat(engine.getNumVersionLookups()).isEqualTo(0);
        assertThat(engine.getNumIndexVersionsLookups()).isEqualTo(0);
        assertThat(engine.getMaxSeenAutoIdTimestamp()).isEqualTo(docs.stream().mapToLong(Engine.Index::getAutoGeneratedIdTimestamp).max().getAsLong());
        assertLuceneOperations(engine, numDocs, 0, 0);
    }

    public static long getNumVersionLookups(InternalEngine engine) { // for other tests to access this
        return engine.getNumVersionLookups();
    }

    public static long getNumIndexVersionsLookups(InternalEngine engine) { // for other tests to access this
        return engine.getNumIndexVersionsLookups();
    }

    @Test
    public void testFailEngineOnRandomIO() throws IOException, InterruptedException {
        MockDirectoryWrapper wrapper = newMockDirectory();
        final Path translogPath = createTempDir("testFailEngineOnRandomIO");
        try (Store store = createStore(wrapper)) {
            CyclicBarrier join = new CyclicBarrier(2);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger controller = new AtomicInteger(0);
            EngineConfig config = config(defaultSettings, store, translogPath, newMergePolicy(), new ReferenceManager.RefreshListener() {
                    @Override
                    public void beforeRefresh() throws IOException {
                    }

                    @Override
                    public void afterRefresh(boolean didRefresh) throws IOException {
                        int i = controller.incrementAndGet();
                        if (i == 1) {
                            throw new MockDirectoryWrapper.FakeIOException();
                        } else if (i == 2) {
                            try {
                                start.await();
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                            throw new ElasticsearchException("something completely different");
                        }
                    }
                });
            InternalEngine internalEngine = createEngine(config);
            int docId = 0;
            final ParsedDocument doc = testParsedDocument(Integer.toString(docId),
                                                          testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())));

            Engine.Index index = randomBoolean() ? indexForDoc(doc) : randomAppendOnly(doc, false, docId);
            internalEngine.index(index);
            Runnable r = () ->  {
                try {
                    join.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                try {
                    internalEngine.refresh("test");
                    fail();
                } catch (AlreadyClosedException ex) {
                    if (ex.getCause() != null) {
                        assertThat(ex.getCause() instanceof MockDirectoryWrapper.FakeIOException).as(ex.toString()).isTrue();
                    }
                } catch (RefreshFailedEngineException ex) {
                    // fine
                } finally {
                    start.countDown();
                }

            };
            Thread t = new Thread(r);
            Thread t1 = new Thread(r);
            t.start();
            t1.start();
            t.join();
            t1.join();
            assertThat(internalEngine.isClosed.get()).isTrue();
            assertThat(internalEngine.failedEngine.get() instanceof MockDirectoryWrapper.FakeIOException).isTrue();
        }
    }

    @Test
    public void testSequenceIDs() throws Exception {
        Tuple<Long, Long> seqID = getSequenceID(engine, new Engine.Get("type", newUid("1")));
        // Non-existent doc returns no seqnum and no primary term
        assertThat(seqID.v1()).isEqualTo(UNASSIGNED_SEQ_NO);
        assertThat(seqID.v2()).isEqualTo(0L);

        // create a document
        Document document = testDocumentWithTextField();
        document.add(new Field(SysColumns.Source.NAME, BytesReference.toBytes(B_1), SysColumns.Source.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", document, B_1);
        engine.index(indexForDoc(doc));
        engine.refresh("test");

        seqID = getSequenceID(engine, newGet(doc));
        logger.info("--> got seqID: {}", seqID);
        assertThat(seqID.v1()).isEqualTo(0L);
        assertThat(seqID.v2()).isEqualTo(primaryTerm.get());

        // Index the same document again
        document = testDocumentWithTextField();
        document.add(new Field(SysColumns.Source.NAME, BytesReference.toBytes(B_1), SysColumns.Source.FIELD_TYPE));
        doc = testParsedDocument("1", document, B_1);
        engine.index(indexForDoc(doc));
        engine.refresh("test");

        seqID = getSequenceID(engine, newGet(doc));
        logger.info("--> got seqID: {}", seqID);
        assertThat(seqID.v1()).isEqualTo(1L);
        assertThat(seqID.v2()).isEqualTo(primaryTerm.get());

        // Index the same document for the third time, this time changing the primary term
        document = testDocumentWithTextField();
        document.add(new Field(SysColumns.Source.NAME, BytesReference.toBytes(B_1), SysColumns.Source.FIELD_TYPE));
        doc = testParsedDocument("1", document, B_1);
        engine.index(new Engine.Index(newUid(doc), doc, UNASSIGNED_SEQ_NO, 3,
                                      Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY,
                                      System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0));
        engine.refresh("test");

        seqID = getSequenceID(engine, newGet(doc));
        logger.info("--> got seqID: {}", seqID);
        assertThat(seqID.v1()).isEqualTo(2L);
        assertThat(seqID.v2()).isEqualTo(3L);

        // we can query by the _seq_no
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        assertThat(searchResult).hasTotalHits(1);
        assertThat(searchResult).hasTotalHits(LongPoint.newExactQuery("_seq_no", 2), 1);
        searchResult.close();
    }

    @Test
    public void testLookupSeqNoByIdInLucene() throws Exception {
        int numOps = between(10, 100);
        long seqNo = 0;
        List<Engine.Operation> operations = new ArrayList<>(numOps);
        for (int i = 0; i < numOps; i++) {
            String id = Integer.toString(between(1, 50));
            boolean isIndexing = randomBoolean();
            int copies = frequently() ? 1 : between(2, 4);
            for (int c = 0; c < copies; c++) {
                final ParsedDocument doc = EngineTestCase.createParsedDoc(id);
                if (isIndexing) {
                    operations.add(new Engine.Index(EngineTestCase.newUid(doc), doc, seqNo, primaryTerm.get(),
                                                    i, null, Engine.Operation.Origin.REPLICA, threadPool.relativeTimeInMillis(), -1, true,  UNASSIGNED_SEQ_NO, 0L));
                } else {
                    operations.add(new Engine.Delete(
                        doc.id(),
                        EngineTestCase.newUid(doc),
                        seqNo,
                        primaryTerm.get(),
                        i,
                        null,
                        Engine.Operation.Origin.REPLICA,
                        threadPool.relativeTimeInMillis(),
                        UNASSIGNED_SEQ_NO,
                        0L
                    ));
                }
            }
            seqNo++;
            if (rarely()) {
                seqNo++;
            }
        }
        Randomness.shuffle(operations);
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        Map<String, Engine.Operation> latestOps = new HashMap<>(); // id -> latest seq_no
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), newMergePolicy(), null))) {
            CheckedRunnable<IOException> lookupAndCheck = () -> {
                try (Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                    Map<String, Long> liveOps = latestOps.entrySet().stream()
                        .filter(e -> e.getValue().operationType() == Engine.Operation.TYPE.INDEX)
                        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().seqNo()));
                    assertThat(getDocIds(engine, true).stream().collect(Collectors.toMap(e -> e.getId(), e -> e.getSeqNo()))).isEqualTo(liveOps);
                    for (String id : latestOps.keySet()) {
                        String msg = "latestOps=" + latestOps + " op=" + id;
                        DocIdAndSeqNo docIdAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.getIndexReader(), newUid(id));
                        if (liveOps.containsKey(id) == false) {
                            assertThat(docIdAndSeqNo).as(msg).isNull();
                        } else {
                            assertThat(docIdAndSeqNo).as(msg).isNotNull();
                            assertThat(docIdAndSeqNo.seqNo).as(msg).isEqualTo(latestOps.get(id).seqNo());
                        }
                    }
                    String notFoundId = randomValueOtherThanMany(liveOps::containsKey, () -> Long.toString(randomNonNegativeLong()));
                    assertThat(VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.getIndexReader(), newUid(notFoundId)))
                        .isNull();
                }
            };
            for (Engine.Operation op : operations) {
                if (op instanceof Engine.Index) {
                    engine.index((Engine.Index) op);
                    if (latestOps.containsKey(op.id()) == false || latestOps.get(op.id()).seqNo() < op.seqNo()) {
                        latestOps.put(op.id(), op);
                    }
                } else if (op instanceof Engine.Delete) {
                    engine.delete((Engine.Delete) op);
                    if (latestOps.containsKey(op.id()) == false || latestOps.get(op.id()).seqNo() < op.seqNo()) {
                        latestOps.put(op.id(), op);
                    }
                }
                if (randomInt(100) < 10) {
                    engine.refresh("test");
                    lookupAndCheck.run();
                }
                if (rarely()) {
                    engine.flush(false, true);
                    lookupAndCheck.run();
                }
            }
            engine.refresh("test");
            lookupAndCheck.run();
        }
    }

    /**
     * A sequence number generator that will generate a sequence number and if {@code stall} is set to true will wait on the barrier and the
     * referenced latch before returning. If the local checkpoint should advance (because {@code stall} is false, then the value of
     * {@code expectedLocalCheckpoint} is set accordingly.
     *
     * @param latchReference          to latch the thread for the purpose of stalling
     * @param barrier                 to signal the thread has generated a new sequence number
     * @param stall                   whether or not the thread should stall
     * @param expectedLocalCheckpoint the expected local checkpoint after generating a new sequence
     *                                number
     * @return a sequence number generator
     */
    private ToLongBiFunction<Engine, Engine.Operation> getStallingSeqNoGenerator(
        final AtomicReference<CountDownLatch> latchReference,
        final CyclicBarrier barrier,
        final AtomicBoolean stall,
        final AtomicLong expectedLocalCheckpoint) {
        return (engine, operation) -> {
            final long seqNo = generateNewSeqNo(engine);
            final CountDownLatch latch = latchReference.get();
            if (stall.get()) {
                try {
                    barrier.await();
                    latch.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                if (expectedLocalCheckpoint.get() + 1 == seqNo) {
                    expectedLocalCheckpoint.set(seqNo);
                }
            }
            return seqNo;
        };
    }

    @Test
    public void testSequenceNumberAdvancesToMaxSeqOnEngineOpenOnPrimary() throws BrokenBarrierException, InterruptedException, IOException {
        engine.close();
        final int docs = randomIntBetween(1, 32);
        InternalEngine initialEngine = null;
        try {
            final AtomicReference<CountDownLatch> latchReference = new AtomicReference<>(new CountDownLatch(1));
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicBoolean stall = new AtomicBoolean();
            final AtomicLong expectedLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            final List<Thread> threads = new ArrayList<>();
            initialEngine =
                createEngine(defaultSettings, store, primaryTranslogDir,
                             newMergePolicy(), null, LocalCheckpointTracker::new, null,
                             getStallingSeqNoGenerator(latchReference, barrier, stall, expectedLocalCheckpoint));
            final InternalEngine finalInitialEngine = initialEngine;
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, testDocumentWithTextField(), SOURCE);

                stall.set(randomBoolean());
                final Thread thread = new Thread(() -> {
                    try {
                        finalInitialEngine.index(indexForDoc(doc));
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
                thread.start();
                if (stall.get()) {
                    threads.add(thread);
                    barrier.await();
                } else {
                    thread.join();
                }
            }

            assertThat(initialEngine.getProcessedLocalCheckpoint()).isEqualTo(expectedLocalCheckpoint.get());
            assertThat(initialEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo((long) (docs - 1));
            initialEngine.flush(true, true);
            assertThat(initialEngine.getPersistedLocalCheckpoint()).isEqualTo(initialEngine.getProcessedLocalCheckpoint());

            latchReference.get().countDown();
            for (final Thread thread : threads) {
                thread.join();
            }
        } finally {
            IOUtils.close(initialEngine);
        }
        try (var recoveringEngine = new InternalEngine(initialEngine.config())) {
            recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
            recoveringEngine.fillSeqNoGaps(2);
            assertThat(recoveringEngine.getPersistedLocalCheckpoint()).isEqualTo(recoveringEngine.getProcessedLocalCheckpoint());
            assertThat(recoveringEngine.getProcessedLocalCheckpoint()).isGreaterThanOrEqualTo((long) (docs - 1));
        }
    }

    @Test
    public void testOutOfOrderSequenceNumbersWithVersionConflict() throws IOException {
        final List<Engine.Operation> operations = new ArrayList<>();

        final int numberOfOperations = randomIntBetween(16, 32);
        final AtomicLong sequenceNumber = new AtomicLong();
        final Engine.Operation.Origin origin = randomFrom(LOCAL_TRANSLOG_RECOVERY, PEER_RECOVERY, PRIMARY, REPLICA);
        final LongSupplier sequenceNumberSupplier =
            origin == PRIMARY ? () -> UNASSIGNED_SEQ_NO : sequenceNumber::getAndIncrement;
        final Supplier<ParsedDocument> doc = () -> {
            final Document document = testDocumentWithTextField();
            document.add(new Field(SysColumns.Source.NAME, BytesReference.toBytes(B_1), SysColumns.Source.FIELD_TYPE));
            return testParsedDocument("1", document, B_1);
        };
        final Term uid = newUid("1");
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        for (int i = 0; i < numberOfOperations; i++) {
            if (randomBoolean()) {
                final Engine.Index index = new Engine.Index(
                    uid,
                    doc.get(),
                    sequenceNumberSupplier.getAsLong(),
                    1,
                    i,
                    origin == PRIMARY ? VersionType.EXTERNAL : null,
                    origin,
                    System.nanoTime(),
                    Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                    false, UNASSIGNED_SEQ_NO, 0);
                operations.add(index);
            } else {
                final Engine.Delete delete = new Engine.Delete(
                    "1",
                    uid,
                    sequenceNumberSupplier.getAsLong(),
                    1,
                    i,
                    origin == PRIMARY ? VersionType.EXTERNAL : null,
                    origin,
                    System.nanoTime(), UNASSIGNED_SEQ_NO, 0);
                operations.add(delete);
            }
        }

        final boolean exists = operations.get(operations.size() - 1) instanceof Engine.Index;
        Randomness.shuffle(operations);

        for (final Engine.Operation operation : operations) {
            if (operation instanceof Engine.Index) {
                engine.index((Engine.Index) operation);
            } else {
                engine.delete((Engine.Delete) operation);
            }
        }

        final long expectedLocalCheckpoint;
        if (origin == PRIMARY) {
            // we can only advance as far as the number of operations that did not conflict
            int count = 0;

            // each time the version increments as we walk the list, that counts as a successful operation
            long version = -1;
            for (int i = 0; i < numberOfOperations; i++) {
                if (operations.get(i).version() >= version) {
                    count++;
                    version = operations.get(i).version();
                }
            }

            // sequence numbers start at zero, so the expected local checkpoint is the number of successful operations minus one
            expectedLocalCheckpoint = count - 1;
        } else {
            expectedLocalCheckpoint = numberOfOperations - 1;
        }

        assertThat(engine.getProcessedLocalCheckpoint()).isEqualTo(expectedLocalCheckpoint);
        try (Engine.GetResult result = engine.get(new Engine.Get("2", uid), searcherFactory)) {
            assertThat(result.docIdAndVersion() != null).isEqualTo(exists);
        }
    }

    /**
     * Test that we do not leak out information on a deleted doc due to it existing in version map. There are at least 2 cases:
     * <ul>
     *     <li>Guessing the deleted seqNo makes the operation succeed</li>
     *     <li>Providing any other seqNo leaks info that the doc was deleted (and its SeqNo)</li>
     * </ul>
     */
    public void testVersionConflictIgnoreDeletedDoc() throws IOException {
        ParsedDocument doc = testParsedDocument("1", testDocument(),
            new BytesArray("{}".getBytes(Charset.defaultCharset())));
        engine.delete(new Engine.Delete("1", newUid("1"), 1));
        for (long seqNo : new long[]{0, 1, randomNonNegativeLong()}) {
            assertDeletedVersionConflict(engine.index(new Engine.Index(newUid("1"), doc, UNASSIGNED_SEQ_NO, 1,
                    Versions.MATCH_ANY, VersionType.INTERNAL,
                    PRIMARY, randomNonNegativeLong(), UNSET_AUTO_GENERATED_TIMESTAMP, false, seqNo, 1)),
                "update: " + seqNo);

            assertDeletedVersionConflict(engine.delete(new Engine.Delete("1", newUid("1"), UNASSIGNED_SEQ_NO, 1,
                    Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, randomNonNegativeLong(), seqNo, 1)),
                "delete: " + seqNo);
        }
    }

    private void assertDeletedVersionConflict(Engine.Result result, String operation) {
        assertThat(result.getFailure()).as("Must have failure for " + operation).isNotNull();
        assertThat(result.getFailure()).as(operation).isExactlyInstanceOf(VersionConflictEngineException.class);
        VersionConflictEngineException exception = (VersionConflictEngineException) result.getFailure();
        assertThat(exception.getMessage()).as(operation).contains("but no document was found");
    }

    /*
     * This test tests that a no-op does not generate a new sequence number, that no-ops can advance the local checkpoint, and that no-ops
     * are correctly added to the translog.
     */
    @Test
    public void testNoOps() throws IOException {
        engine.close();
        InternalEngine noOpEngine = null;
        final int maxSeqNo = randomIntBetween(0, 128);
        final int localCheckpoint = randomIntBetween(0, maxSeqNo);
        try {
            final BiFunction<Long, Long, LocalCheckpointTracker> supplier = (ms, lcp) -> new LocalCheckpointTracker(
                maxSeqNo,
                localCheckpoint);
            EngineConfig noopEngineConfig = copy(engine.config(), new SoftDeletesRetentionMergePolicy(
                Lucene.SOFT_DELETES_FIELD,
                () -> new MatchAllDocsQuery(), engine.config().getMergePolicy()));
            noOpEngine = new InternalEngine(noopEngineConfig, IndexWriter.MAX_DOCS, supplier) {
                @Override
                protected long doGenerateSeqNoForOperation(Operation operation) {
                    throw new UnsupportedOperationException();
                }
            };
            noOpEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
            final int gapsFilled = noOpEngine.fillSeqNoGaps(primaryTerm.get());
            final String reason = "filling gaps";
            noOpEngine.noOp(new Engine.NoOp(maxSeqNo + 1, primaryTerm.get(), LOCAL_TRANSLOG_RECOVERY, System.nanoTime(), reason));
            assertThat(noOpEngine.getProcessedLocalCheckpoint()).isEqualTo((long) (maxSeqNo + 1));
            assertThat(noOpEngine.getTranslog().stats().getUncommittedOperations()).isEqualTo(gapsFilled);
            noOpEngine.noOp(
                new Engine.NoOp(maxSeqNo + 2, primaryTerm.get(),
                    randomFrom(PRIMARY, REPLICA, PEER_RECOVERY), System.nanoTime(), reason));
            assertThat(noOpEngine.getProcessedLocalCheckpoint()).isEqualTo((long) (maxSeqNo + 2));
            assertThat(noOpEngine.getTranslog().stats().getUncommittedOperations()).isEqualTo(gapsFilled + 1);
            // skip to the op that we added to the translog
            Translog.Operation op;
            Translog.Operation last = null;
            try (Translog.Snapshot snapshot = noOpEngine.getTranslog().newSnapshot()) {
                while ((op = snapshot.next()) != null) {
                    last = op;
                }
            }
            assertThat(last).isNotNull();
            assertThat(last).isExactlyInstanceOf(Translog.NoOp.class);
            final Translog.NoOp noOp = (Translog.NoOp) last;
            assertThat(noOp.seqNo()).isEqualTo((long) (maxSeqNo + 2));
            assertThat(noOp.primaryTerm()).isEqualTo(primaryTerm.get());
            assertThat(noOp.reason()).isEqualTo(reason);
            if (engine.engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
                List<Translog.Operation> operationsFromLucene = readAllOperationsInLucene(noOpEngine);
                assertThat(operationsFromLucene).hasSize(maxSeqNo + 2 - localCheckpoint); // fills n gap and 2 manual noop.
                for (int i = 0; i < operationsFromLucene.size(); i++) {
                    assertThat(operationsFromLucene.get(i)).isEqualTo(new Translog.NoOp(localCheckpoint + 1 + i, primaryTerm.get(), "filling gaps"));
                }
                assertConsistentHistoryBetweenTranslogAndLuceneIndex(noOpEngine);
            }
        } finally {
            IOUtils.close(noOpEngine);
        }
    }

    /**
     * Verifies that a segment containing only no-ops can be used to look up _version and _seqno.
     */
    @Test
    public void testSegmentContainsOnlyNoOps() throws Exception {
        final long seqNo = randomLongBetween(0, 1000);
        final long term = this.primaryTerm.get();
        Engine.NoOpResult noOpResult = engine.noOp(new Engine.NoOp(
            seqNo,
            term,
            randomFrom(Engine.Operation.Origin.values()),
            randomNonNegativeLong(),
            "test")
        );
        assertThat(noOpResult.getFailure()).isNull();
        assertThat(noOpResult.getSeqNo()).isEqualTo(seqNo);
        assertThat(noOpResult.getTerm()).isEqualTo(term);
        engine.refresh("test");
        Engine.DeleteResult deleteResult = engine.delete(replicaDeleteForDoc("id", 1, seqNo + between(1, 1000), randomNonNegativeLong()));
        assertThat(deleteResult.getFailure()).isNull();
        engine.refresh("test");
    }

    /**
     * A simple test to check that random combination of operations can coexist in segments and be lookup.
     * This is needed as some fields in Lucene may not exist if a segment misses operation types and this code is to check for that.
     * For example, a segment containing only no-ops does not have neither _uid or _version.
     */
    @Test
    public void testRandomOperations() throws Exception {
        int numOps = between(10, 100);
        for (int i = 0; i < numOps; i++) {
            String id = Integer.toString(randomIntBetween(1, 10));
            ParsedDocument doc = createParsedDoc(id);
            Engine.Operation.TYPE type = randomFrom(Engine.Operation.TYPE.values());
            switch (type) {
                case INDEX:
                    Engine.IndexResult index = engine.index(replicaIndexForDoc(doc, between(1, 100), i, randomBoolean()));
                    assertThat(index.getFailure()).isNull();
                    break;
                case DELETE:
                    Engine.DeleteResult delete = engine.delete(replicaDeleteForDoc(doc.id(), between(1, 100), i, randomNonNegativeLong()));
                    assertThat(delete.getFailure()).isNull();
                    break;
                case NO_OP:
                    long seqNo = i;
                    Engine.NoOpResult noOp = engine.noOp(new Engine.NoOp(seqNo, primaryTerm.get(),
                        randomFrom(Engine.Operation.Origin.values()), randomNonNegativeLong(), ""));
                    assertThat(noOp.getTerm()).isEqualTo(primaryTerm.get());
                    assertThat(noOp.getSeqNo()).isEqualTo(seqNo);
                    assertThat(noOp.getFailure()).isNull();
                    break;
                default:
                    throw new IllegalStateException("Invalid op [" + type + "]");
            }
            if (randomBoolean()) {
                engine.refresh("test");
            }
            if (randomBoolean()) {
                engine.flush();
            }
            if (randomBoolean()) {
                engine.forceMerge(randomBoolean(), between(1, 10), randomBoolean(), UUIDs.randomBase64UUID());
            }
        }
        if (engine.engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            List<Translog.Operation> operations = readAllOperationsInLucene(engine);
            assertThat(operations).hasSize(numOps);
        }
    }

    @Test
    public void testMinGenerationForSeqNo() throws IOException, BrokenBarrierException, InterruptedException {
        engine.close();
        final int numberOfTriplets = randomIntBetween(1, 32);
        InternalEngine actualEngine = null;
        try {
            final AtomicReference<CountDownLatch> latchReference = new AtomicReference<>();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicBoolean stall = new AtomicBoolean();
            final AtomicLong expectedLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            final Map<Thread, CountDownLatch> threads = new LinkedHashMap<>();
            actualEngine =
                createEngine(defaultSettings, store, primaryTranslogDir,
                             newMergePolicy(), null, LocalCheckpointTracker::new, null,
                             getStallingSeqNoGenerator(latchReference, barrier, stall, expectedLocalCheckpoint));
            final InternalEngine finalActualEngine = actualEngine;
            final Translog translog = finalActualEngine.getTranslog();
            final long generation = finalActualEngine.getTranslog().currentFileGeneration();
            for (int i = 0; i < numberOfTriplets; i++) {
                /*
                 * Index three documents with the first and last landing in the same generation and the middle document being stalled until
                 * a later generation.
                 */
                stall.set(false);
                index(finalActualEngine, 3 * i);

                final CountDownLatch latch = new CountDownLatch(1);
                latchReference.set(latch);
                final int skipId = 3 * i + 1;
                stall.set(true);
                final Thread thread = new Thread(() -> {
                    try {
                        index(finalActualEngine, skipId);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
                thread.start();
                threads.put(thread, latch);
                barrier.await();

                stall.set(false);
                index(finalActualEngine, 3 * i + 2);
                finalActualEngine.flush();

                /*
                 * This sequence number landed in the last generation, but the lower and upper bounds for an earlier generation straddle
                 * this sequence number.
                 */
                assertThat(translog.getMinGenerationForSeqNo(3 * i + 1).translogFileGeneration).isEqualTo(i + generation);
            }

            int i = 0;
            for (final Map.Entry<Thread, CountDownLatch> entry : threads.entrySet()) {
                final Map<String, String> userData = finalActualEngine.commitStats().getUserData();
                assertThat(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)).isEqualTo(Long.toString(3 * i));
                entry.getValue().countDown();
                entry.getKey().join();
                finalActualEngine.flush();
                i++;
            }

        } finally {
            IOUtils.close(actualEngine);
        }
    }

    private void index(final InternalEngine engine, final int id) throws IOException {
        final String docId = Integer.toString(id);
        final ParsedDocument doc =
            testParsedDocument(docId, testDocumentWithTextField(), SOURCE);
        engine.index(indexForDoc(doc));
    }

    /**
     * Return a tuple representing the sequence ID for the given {@code Get}
     * operation. The first value in the tuple is the sequence number, the
     * second is the primary term.
     */
    private Tuple<Long, Long> getSequenceID(Engine engine, Engine.Get get) throws EngineException {
        try (Searcher searcher = engine.acquireSearcher("get", Engine.SearcherScope.INTERNAL)) {
            final long primaryTerm;
            final long seqNo;
            DocIdAndSeqNo docIdAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.getIndexReader(), get.uid());
            if (docIdAndSeqNo == null) {
                primaryTerm = 0;
                seqNo = UNASSIGNED_SEQ_NO;
            } else {
                seqNo = docIdAndSeqNo.seqNo;
                NumericDocValues primaryTerms = docIdAndSeqNo.context.reader().getNumericDocValues(SysColumns.Names.PRIMARY_TERM);
                if (primaryTerms == null || primaryTerms.advanceExact(docIdAndSeqNo.docId) == false) {
                    throw new AssertionError("document does not have primary term [" + docIdAndSeqNo.docId + "]");
                }
                primaryTerm = primaryTerms.longValue();
            }
            return new Tuple<>(seqNo, primaryTerm);
        } catch (Exception e) {
            throw new EngineException(shardId, "unable to retrieve sequence id", e);
        }
    }

    @Test
    public void testRestoreLocalHistoryFromTranslog() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            final ArrayList<Long> seqNos = new ArrayList<>();
            final int numOps = randomIntBetween(0, 1024);
            for (int i = 0; i < numOps; i++) {
                if (rarely()) {
                    continue;
                }
                seqNos.add((long) i);
            }
            Randomness.shuffle(seqNos);
            final EngineConfig engineConfig;
            final SeqNoStats prevSeqNoStats;
            final List<DocIdSeqNoAndSource> prevDocs;
            try (InternalEngine engine = createEngine(store, createTempDir(), globalCheckpoint::get)) {
                engineConfig = engine.config();
                for (final long seqNo : seqNos) {
                    final String id = Long.toString(seqNo);
                    final ParsedDocument doc = testParsedDocument(
                        id, testDocumentWithTextField(), SOURCE);
                    engine.index(replicaIndexForDoc(doc, 1, seqNo, false));
                    if (rarely()) {
                        engine.rollTranslogGeneration();
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                }
                globalCheckpoint.set(randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, engine.getPersistedLocalCheckpoint()));
                engine.syncTranslog();
                prevSeqNoStats = engine.getSeqNoStats(globalCheckpoint.get());
                prevDocs = getDocIds(engine, true);
            }
            try (InternalEngine engine = new InternalEngine(engineConfig)) {
                final long currentTranslogGeneration = engine.getTranslog().currentFileGeneration();
                engine.recoverFromTranslog(translogHandler, globalCheckpoint.get());
                engine.restoreLocalHistoryFromTranslog(translogHandler);
                assertThat(getDocIds(engine, true)).isEqualTo(prevDocs);
                SeqNoStats seqNoStats = engine.getSeqNoStats(globalCheckpoint.get());
                assertThat(seqNoStats.getLocalCheckpoint()).isEqualTo(prevSeqNoStats.getLocalCheckpoint());
                assertThat(seqNoStats.getMaxSeqNo()).isEqualTo(prevSeqNoStats.getMaxSeqNo());
                assertThat(engine.getTranslog().totalOperationsByMinGen(currentTranslogGeneration)).as("restore from local translog must not add operations to translog").isEqualTo(0);
            }
            assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
        }
    }

    @Test
    public void testFillUpSequenceIdGapsOnRecovery() throws IOException {
        final int docs = randomIntBetween(1, 32);
        int numDocsOnReplica = 0;
        long maxSeqIDOnReplica = -1;
        long checkpointOnReplica;
        try {
            for (int i = 0; i < docs; i++) {
                final String docId = Integer.toString(i);
                final ParsedDocument doc =
                    testParsedDocument(docId, testDocumentWithTextField(), SOURCE);
                Engine.Index primaryResponse = indexForDoc(doc);
                Engine.IndexResult indexResult = engine.index(primaryResponse);
                if (randomBoolean()) {
                    numDocsOnReplica++;
                    maxSeqIDOnReplica = indexResult.getSeqNo();
                    replicaEngine.index(replicaIndexForDoc(doc, 1, indexResult.getSeqNo(), false));
                }
            }
            engine.syncTranslog(); // to advance local checkpoint
            replicaEngine.syncTranslog(); // to advance local checkpoint
            checkpointOnReplica = replicaEngine.getProcessedLocalCheckpoint();
        } finally {
            IOUtils.close(replicaEngine);
        }

        boolean flushed = false;
        AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        InternalEngine recoveringEngine = null;
        try {
            assertThat(engine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(docs - 1);
            assertThat(engine.getProcessedLocalCheckpoint()).isEqualTo(docs - 1);
            assertThat(replicaEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(maxSeqIDOnReplica);
            assertThat(replicaEngine.getProcessedLocalCheckpoint()).isEqualTo(checkpointOnReplica);
            recoveringEngine = new InternalEngine(copy(replicaEngine.config(), globalCheckpoint::get));
            assertThat(getTranslog(recoveringEngine).stats().getUncommittedOperations()).isEqualTo(numDocsOnReplica);
            recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
            assertThat(recoveringEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(maxSeqIDOnReplica);
            assertThat(recoveringEngine.getProcessedLocalCheckpoint()).isEqualTo(checkpointOnReplica);
            assertThat(recoveringEngine.fillSeqNoGaps(2)).isEqualTo((maxSeqIDOnReplica + 1) - numDocsOnReplica);

            // now snapshot the tlog and ensure the primary term is updated
            try (Translog.Snapshot snapshot = getTranslog(recoveringEngine).newSnapshot()) {
                assertThat((maxSeqIDOnReplica + 1) - numDocsOnReplica <= snapshot.totalOperations()).isTrue();
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    if (operation.opType() == Translog.Operation.Type.NO_OP) {
                        assertThat(operation.primaryTerm()).isEqualTo(2);
                    } else {
                        assertThat(operation.primaryTerm()).isEqualTo(primaryTerm.get());
                    }

                }
                assertThat(recoveringEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(maxSeqIDOnReplica);
                assertThat(recoveringEngine.getProcessedLocalCheckpoint()).isEqualTo(maxSeqIDOnReplica);
                if ((flushed = randomBoolean())) {
                    globalCheckpoint.set(recoveringEngine.getSeqNoStats(-1).getMaxSeqNo());
                    getTranslog(recoveringEngine).sync();
                    recoveringEngine.flush(true, true);
                }
            }
        } finally {
            IOUtils.close(recoveringEngine);
        }

        // now do it again to make sure we preserve values etc.
        try {
            recoveringEngine = new InternalEngine(copy(replicaEngine.config(), globalCheckpoint::get));
            if (flushed) {
                assertThat(recoveringEngine.getTranslogStats().getUncommittedOperations()).isEqualTo(0);
            }
            recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
            assertThat(recoveringEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(maxSeqIDOnReplica);
            assertThat(recoveringEngine.getProcessedLocalCheckpoint()).isEqualTo(maxSeqIDOnReplica);
            assertThat(recoveringEngine.fillSeqNoGaps(3)).isEqualTo(0);
            assertThat(recoveringEngine.getSeqNoStats(-1).getMaxSeqNo()).isEqualTo(maxSeqIDOnReplica);
            assertThat(recoveringEngine.getProcessedLocalCheckpoint()).isEqualTo(maxSeqIDOnReplica);
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    public void assertSameReader(Searcher left, Searcher right) {
        List<LeafReaderContext> leftLeaves = ElasticsearchDirectoryReader.unwrap(left.getDirectoryReader()).leaves();
        List<LeafReaderContext> rightLeaves = ElasticsearchDirectoryReader.unwrap(right.getDirectoryReader()).leaves();
        assertThat(leftLeaves.size()).isEqualTo(rightLeaves.size());
        for (int i = 0; i < leftLeaves.size(); i++) {
            assertThat(leftLeaves.get(i).reader()).isSameAs(rightLeaves.get(i).reader());
        }
    }

    public void assertNotSameReader(Searcher left, Searcher right) {
        List<LeafReaderContext> leftLeaves = ElasticsearchDirectoryReader.unwrap(left.getDirectoryReader()).leaves();
        List<LeafReaderContext> rightLeaves = ElasticsearchDirectoryReader.unwrap(right.getDirectoryReader()).leaves();
        if (rightLeaves.size() == leftLeaves.size()) {
            for (int i = 0; i < leftLeaves.size(); i++) {
                if (leftLeaves.get(i).reader() != rightLeaves.get(i).reader()) {
                    return; // all is well
                }
            }
            fail("readers are same");
        }
    }

    @Test
    public void testRefreshScopedSearcher() throws IOException {
        try (Store store = createStore();
             InternalEngine engine =
                 // disable merges to make sure that the reader doesn't change unexpectedly during the test
                 createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {

            engine.refresh("warm_up");
            try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                 Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                assertSameReader(getSearcher, searchSearcher);
            }
            for (int i = 0; i < 10; i++) {
                final String docId = Integer.toString(i);
                final ParsedDocument doc =
                    testParsedDocument(docId, testDocumentWithTextField(), SOURCE);
                Engine.Index primaryResponse = indexForDoc(doc);
                engine.index(primaryResponse);
            }
            assertThat(engine.refreshNeeded()).isTrue();
            engine.refresh("test", Engine.SearcherScope.INTERNAL, true);
            try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                 Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                assertThat(getSearcher.getIndexReader().numDocs()).isEqualTo(10);
                assertThat(searchSearcher.getIndexReader().numDocs()).isEqualTo(0);
                assertNotSameReader(getSearcher, searchSearcher);
            }
            engine.refresh("test", Engine.SearcherScope.EXTERNAL, true);

            try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                 Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                assertThat(getSearcher.getIndexReader().numDocs()).isEqualTo(10);
                assertThat(searchSearcher.getIndexReader().numDocs()).isEqualTo(10);
                assertSameReader(getSearcher, searchSearcher);
            }

            // now ensure external refreshes are reflected on the internal reader
            final String docId = Integer.toString(10);
            final ParsedDocument doc =
                testParsedDocument(docId, testDocumentWithTextField(), SOURCE);
            Engine.Index primaryResponse = indexForDoc(doc);
            engine.index(primaryResponse);

            engine.refresh("test", Engine.SearcherScope.EXTERNAL, true);

            try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                 Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                assertThat(getSearcher.getIndexReader().numDocs()).isEqualTo(11);
                assertThat(searchSearcher.getIndexReader().numDocs()).isEqualTo(11);
                assertSameReader(getSearcher, searchSearcher);
            }

            try (Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                engine.refresh("test", Engine.SearcherScope.INTERNAL, true);
                try (Searcher nextSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                    assertThat(nextSearcher.getIndexReader()).isSameAs(searcher.getIndexReader());
                }
            }

            try (Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                engine.refresh("test", Engine.SearcherScope.EXTERNAL, true);
                try (Searcher nextSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                    assertThat(nextSearcher.getIndexReader()).isSameAs(searcher.getIndexReader());
                }
            }
        }
    }

    @Test
    public void testSeqNoGenerator() throws IOException {
        engine.close();
        final long seqNo = randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Integer.MAX_VALUE);
        final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier = (ms, lcp) -> new LocalCheckpointTracker(
            SequenceNumbers.NO_OPS_PERFORMED,
            SequenceNumbers.NO_OPS_PERFORMED);
        final AtomicLong seqNoGenerator = new AtomicLong(seqNo);
        try (Engine e = createEngine(defaultSettings, store, primaryTranslogDir,
                                     newMergePolicy(), null, localCheckpointTrackerSupplier,
                                     null, (engine, operation) -> seqNoGenerator.getAndIncrement())) {
            final String id = "id";
            final Field uidField = new Field("_id", id, SysColumns.ID.FIELD_TYPE);
            final Field versionField = new NumericDocValuesField("_version", 0);
            final SequenceIDFields seqID = SequenceIDFields.emptySeqID();
            final Document document = new Document();
            document.add(uidField);
            document.add(versionField);
            document.add(seqID.seqNo);
            document.add(seqID.seqNoDocValue);
            document.add(seqID.primaryTerm);
            final BytesReference source = new BytesArray(new byte[]{1});
            final ParsedDocument parsedDocument = new ParsedDocument(
                versionField,
                seqID,
                id,
                document,
                source
            );

            final Engine.Index index = new Engine.Index(
                new Term("_id", parsedDocument.id()),
                parsedDocument,
                UNASSIGNED_SEQ_NO,
                randomIntBetween(1, 8),
                Versions.NOT_FOUND,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                -1,
                randomBoolean(),
                UNASSIGNED_SEQ_NO,
                0);
            final Engine.IndexResult indexResult = e.index(index);
            assertThat(indexResult.getSeqNo()).isEqualTo(seqNo);
            assertThat(seqNoGenerator.get()).isEqualTo(seqNo + 1);

            final Engine.Delete delete = new Engine.Delete(
                id,
                new Term("_id", parsedDocument.id()),
                UNASSIGNED_SEQ_NO,
                randomIntBetween(1, 8),
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                UNASSIGNED_SEQ_NO,
                0);
            final Engine.DeleteResult deleteResult = e.delete(delete);
            assertThat(deleteResult.getSeqNo()).isEqualTo(seqNo + 1);
            assertThat(seqNoGenerator.get()).isEqualTo(seqNo + 2);
        }
    }

    @Test
    public void testKeepTranslogAfterGlobalCheckpoint() throws Exception {
        IOUtils.close(engine, store);
        final IndexSettings indexSettings = new IndexSettings(defaultSettings.getIndexMetadata(), defaultSettings.getNodeSettings(),
                                                              defaultSettings.getScopedSettings());
        IndexMetadata.Builder builder = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(Settings.builder().put(indexSettings.getSettings())
                          .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomFrom("-1", "100micros", "30m"))
                          .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), randomFrom("-1", "512b", "1gb")));
        indexSettings.updateIndexMetadata(builder.build());

        final Path translogPath = createTempDir();
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        store.createEmpty(Version.CURRENT.luceneVersion);
        final String translogUUID = Translog.createEmptyTranslog(translogPath, globalCheckpoint.get(), shardId, primaryTerm.get());
        store.associateIndexWithNewTranslog(translogUUID);

        final EngineConfig engineConfig = config(indexSettings, store, translogPath,
                                                 NoMergePolicy.INSTANCE, null, null, () -> globalCheckpoint.get());
        final AtomicLong lastSyncedGlobalCheckpointBeforeCommit = new AtomicLong(Translog.readGlobalCheckpoint(translogPath, translogUUID));
        try (InternalEngine engine = new InternalEngine(engineConfig) {
            @Override
            protected void commitIndexWriter(IndexWriter writer, Translog translog, String syncId) throws IOException {
                lastSyncedGlobalCheckpointBeforeCommit.set(Translog.readGlobalCheckpoint(translogPath, translogUUID));
                // Advance the global checkpoint during the flush to create a lag between a persisted global checkpoint in the translog
                // (this value is visible to the deletion policy) and an in memory global checkpoint in the SequenceNumbersService.
                if (rarely()) {
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), getPersistedLocalCheckpoint()));
                }
                super.commitIndexWriter(writer, translog, syncId);
            }
        }) {
            engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
            int numDocs = scaledRandomIntBetween(10, 100);
            for (int docId = 0; docId < numDocs; docId++) {
                Document document = testDocumentWithTextField();
                document.add(new Field(SysColumns.Source.NAME, BytesReference.toBytes(B_1), SysColumns.Source.FIELD_TYPE));
                engine.index(indexForDoc(testParsedDocument(Integer.toString(docId), document, B_1)));
                if (frequently()) {
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                    engine.syncTranslog();
                }
                if (frequently()) {
                    engine.flush(randomBoolean(), true);
                    final List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
                    // Keep only one safe commit as the oldest commit.
                    final IndexCommit safeCommit = commits.get(0);
                    if (lastSyncedGlobalCheckpointBeforeCommit.get() == UNASSIGNED_SEQ_NO) {
                        // If the global checkpoint is still unassigned, we keep an empty(eg. initial) commit as a safe commit.
                        assertThat(Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO))).isEqualTo(SequenceNumbers.NO_OPS_PERFORMED);
                    } else {
                        assertThat(Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)))
                            .isLessThanOrEqualTo(lastSyncedGlobalCheckpointBeforeCommit.get());
                    }
                    for (int i = 1; i < commits.size(); i++) {
                        assertThat(Long.parseLong(commits.get(i).getUserData().get(SequenceNumbers.MAX_SEQ_NO)))
                            .isGreaterThan(lastSyncedGlobalCheckpointBeforeCommit.get());
                    }
                    // Make sure we keep all translog operations after the local checkpoint of the safe commit.
                    long localCheckpointFromSafeCommit = Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
                    try (Translog.Snapshot snapshot = getTranslog(engine).newSnapshot()) {
                        assertThat(snapshot).containsSeqNoRange(localCheckpointFromSafeCommit + 1, docId);
                    }
                }
            }
        }
    }

    @Test
    public void testConcurrentAppendUpdateAndRefresh() throws InterruptedException, IOException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicInteger numDeletes = new AtomicInteger();
        Thread thread = new Thread(() -> {
            try {
                latch.countDown();
                latch.await();
                for (int j = 0; j < numDocs; j++) {
                    String docID = Integer.toString(j);
                    ParsedDocument doc = testParsedDocument(docID, testDocumentWithTextField(),
                                                            new BytesArray("{}".getBytes(Charset.defaultCharset())));
                    Engine.Index operation = appendOnlyPrimary(doc, false, 1);
                    engine.index(operation);
                    if (rarely()) {
                        engine.delete(new Engine.Delete(
                            operation.id(),
                            operation.uid(),
                            UNASSIGNED_SEQ_NO,
                            primaryTerm.get(),
                            Versions.MATCH_ANY,
                            VersionType.INTERNAL,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            UNASSIGNED_SEQ_NO,
                            0
                        ));
                        numDeletes.incrementAndGet();
                    } else {
                        doc = testParsedDocument(docID, testDocumentWithTextField("updated"),
                                                 new BytesArray("{}".getBytes(Charset.defaultCharset())));
                        Engine.Index update = indexForDoc(doc);
                        engine.index(update);
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                done.set(true);
            }
        });
        thread.start();
        latch.countDown();
        latch.await();
        while (done.get() == false) {
            engine.refresh("test", Engine.SearcherScope.INTERNAL, true);
        }
        thread.join();
        engine.refresh("test", Engine.SearcherScope.INTERNAL, true);
        try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            TopDocs search = searcher.search(new MatchAllDocsQuery(), searcher.getIndexReader().numDocs());
            for (int i = 0; i < search.scoreDocs.length; i++) {
                org.apache.lucene.document.Document luceneDoc = searcher.doc(search.scoreDocs[i].doc);
                assertThat(luceneDoc.get("value")).isEqualTo("updated");
            }
            int totalNumDocs = numDocs - numDeletes.get();
            assertThat(searcher.getIndexReader().numDocs()).isEqualTo(totalNumDocs);
        }
    }

    @Test
    public void testAcquireIndexCommit() throws Exception {
        IOUtils.close(engine, store);
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final Engine.IndexCommitRef snapshot;
        final boolean closeSnapshotBeforeEngine = randomBoolean();
        try (InternalEngine engine = createEngine(store, createTempDir(), globalCheckpoint::get)) {
            int numDocs = between(1, 20);
            for (int i = 0; i < numDocs; i++) {
                index(engine, i);
            }
            if (randomBoolean()) {
                globalCheckpoint.set(numDocs - 1);
            }
            final boolean flushFirst = randomBoolean();
            final boolean safeCommit = randomBoolean();
            if (safeCommit) {
                snapshot = engine.acquireSafeIndexCommit();
            } else {
                snapshot = engine.acquireLastIndexCommit(flushFirst);
            }
            int moreDocs = between(1, 20);
            for (int i = 0; i < moreDocs; i++) {
                index(engine, numDocs + i);
            }
            globalCheckpoint.set(numDocs + moreDocs - 1);
            engine.flush();
            // check that we can still read the commit that we captured
            try (IndexReader reader = DirectoryReader.open(snapshot.getIndexCommit())) {
                assertThat(reader.numDocs()).isEqualTo(flushFirst && safeCommit == false ? numDocs : 0);
            }
            assertThat(DirectoryReader.listCommits(engine.store.directory())).hasSize(2);

            if (closeSnapshotBeforeEngine) {
                snapshot.close();
                // check it's clean up
                engine.flush(true, true);
                assertThat(DirectoryReader.listCommits(engine.store.directory())).hasSize(1);
            }
        }

        if (closeSnapshotBeforeEngine == false) {
            snapshot.close(); // shouldn't throw AlreadyClosedException
        }
    }

    @Test
    public void testCleanUpCommitsWhenGlobalCheckpointAdvanced() throws Exception {
        IOUtils.close(engine, store);
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test",
                                                                                 Settings.builder().put(defaultSettings.getSettings())
                                                                                     .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), -1)
                                                                                     .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), -1).build());
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore();
             InternalEngine engine =
                 createEngine(config(indexSettings, store, createTempDir(), newMergePolicy(),
                                     null, null, globalCheckpoint::get))) {
            final int numDocs = scaledRandomIntBetween(10, 100);
            for (int docId = 0; docId < numDocs; docId++) {
                index(engine, docId);
                if (rarely()) {
                    engine.flush(randomBoolean(), true);
                }
            }
            engine.flush(false, randomBoolean());
            globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
            engine.syncTranslog();
            List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
            assertThat(Long.parseLong(commits.get(0).getUserData().get(SequenceNumbers.MAX_SEQ_NO)))
                .isLessThanOrEqualTo(globalCheckpoint.get());
            for (int i = 1; i < commits.size(); i++) {
                assertThat(Long.parseLong(commits.get(i).getUserData().get(SequenceNumbers.MAX_SEQ_NO)))
                    .isGreaterThan(globalCheckpoint.get());
            }
            // Global checkpoint advanced enough - only the last commit is kept.
            globalCheckpoint.set(randomLongBetween(engine.getPersistedLocalCheckpoint(), Long.MAX_VALUE));
            engine.syncTranslog();
            assertThat(DirectoryReader.listCommits(store.directory())).containsExactly(commits.get(commits.size() - 1));
            assertThat(engine.getTranslog().totalOperations()).isEqualTo(0);
        }
    }

    @Test
    public void testCleanupCommitsWhenReleaseSnapshot() throws Exception {
        IOUtils.close(engine, store);
        store = createStore();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (InternalEngine engine = createEngine(store, createTempDir(), globalCheckpoint::get)) {
            final int numDocs = scaledRandomIntBetween(10, 100);
            for (int docId = 0; docId < numDocs; docId++) {
                index(engine, docId);
                if (frequently()) {
                    engine.flush(randomBoolean(), true);
                }
            }
            engine.flush(false, randomBoolean());
            int numSnapshots = between(1, 10);
            final List<Engine.IndexCommitRef> snapshots = new ArrayList<>();
            for (int i = 0; i < numSnapshots; i++) {
                snapshots.add(engine.acquireSafeIndexCommit()); // taking snapshots from the safe commit.
            }
            globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
            engine.syncTranslog();
            final List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
            for (int i = 0; i < numSnapshots - 1; i++) {
                snapshots.get(i).close();
                // pending snapshots - should not release any commit.
                assertThat(DirectoryReader.listCommits(store.directory())).isEqualTo(commits);
            }
            snapshots.get(numSnapshots - 1).close(); // release the last snapshot - delete all except the last commit
            assertThat(DirectoryReader.listCommits(store.directory())).hasSize(1);
        }
    }

    @Test
    public void testShouldPeriodicallyFlush() throws Exception {
        assertThat(engine.shouldPeriodicallyFlush()).as("Empty engine does not need flushing").isFalse();
        // A new engine may have more than one empty translog files - the test should account this extra.
        final Translog translog = engine.getTranslog();
        final IntSupplier uncommittedTranslogOperationsSinceLastCommit = () -> {
            long localCheckpoint = Long.parseLong(engine.getLastCommittedSegmentInfos().userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
            return translog.totalOperationsByMinGen(translog.getMinGenerationForSeqNo(localCheckpoint + 1).translogFileGeneration);
        };
        final long extraTranslogSizeInNewEngine =
            engine.getTranslog().stats().getUncommittedSizeInBytes() - Translog.DEFAULT_HEADER_SIZE_IN_BYTES;
        int numDocs = between(10, 100);
        for (int id = 0; id < numDocs; id++) {
            final ParsedDocument doc =
                testParsedDocument(Integer.toString(id), testDocumentWithTextField(), SOURCE);
            engine.index(indexForDoc(doc));
        }
        assertThat(engine.shouldPeriodicallyFlush()).as("Not exceeded translog flush threshold yet").isFalse();
        long flushThreshold = RandomNumbers.randomLongBetween(random(), 120,
            engine.getTranslog().stats().getUncommittedSizeInBytes()- extraTranslogSizeInNewEngine);
        final IndexSettings indexSettings = engine.config().getIndexSettings();
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(Settings.builder().put(indexSettings.getSettings())
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), flushThreshold + "b")).build();
        indexSettings.updateIndexMetadata(indexMetadata);
        engine.onSettingsChanged(indexSettings.getTranslogRetentionAge(), indexSettings.getTranslogRetentionSize(),
            indexSettings.getSoftDeleteRetentionOperations());
        assertThat(engine.getTranslog().stats().getUncommittedOperations()).isEqualTo(numDocs);
        assertThat(engine.shouldPeriodicallyFlush()).isTrue();
        engine.flush();
        assertThat(uncommittedTranslogOperationsSinceLastCommit.getAsInt()).isEqualTo(0);
        // Stale operations skipped by Lucene but added to translog - still able to flush
        for (int id = 0; id < numDocs; id++) {
            final ParsedDocument doc =
                testParsedDocument(Integer.toString(id), testDocumentWithTextField(), SOURCE);
            final Engine.IndexResult result = engine.index(replicaIndexForDoc(doc, 1L, id, false));
            assertThat(result.isCreated()).isFalse();
        }
        SegmentInfos lastCommitInfo = engine.getLastCommittedSegmentInfos();
        assertThat(uncommittedTranslogOperationsSinceLastCommit.getAsInt()).isEqualTo(numDocs);
        assertThat(engine.shouldPeriodicallyFlush()).isTrue();
        engine.flush(false, false);
        assertThat(engine.getLastCommittedSegmentInfos()).isNotSameAs(lastCommitInfo);
        assertThat(uncommittedTranslogOperationsSinceLastCommit.getAsInt()).isEqualTo(0);
        // If the new index commit still points to the same translog generation as the current index commit,
        // we should not enable the periodically flush condition; otherwise we can get into an infinite loop of flushes.
        generateNewSeqNo(engine); // create a gap here
        for (int id = 0; id < numDocs; id++) {
            if (randomBoolean()) {
                translog.rollGeneration();
            }
            final ParsedDocument doc =
                testParsedDocument("new" + id, testDocumentWithTextField(), SOURCE);
            engine.index(replicaIndexForDoc(doc, 2L, generateNewSeqNo(engine), false));
            if (engine.shouldPeriodicallyFlush()) {
                engine.flush();
                assertThat(engine.getLastCommittedSegmentInfos()).isNotSameAs(lastCommitInfo);
                assertThat(engine.shouldPeriodicallyFlush()).isFalse();
            }
        }
    }

    @Test
    public void testShouldPeriodicallyFlushAfterMerge() throws Exception {
        assertThat(engine.shouldPeriodicallyFlush()).as("Empty engine does not need flushing").isFalse();
        ParsedDocument doc =
            testParsedDocument(Integer.toString(0), testDocumentWithTextField(), SOURCE);
        engine.index(indexForDoc(doc));
        engine.refresh("test");
        assertThat(engine.shouldPeriodicallyFlush()).as("Not exceeded translog flush threshold yet").isFalse();
        final IndexSettings indexSettings = engine.config().getIndexSettings();
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(Settings.builder().put(indexSettings.getSettings())
                          .put(IndexSettings.INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING.getKey(),  "0b")).build();
        indexSettings.updateIndexMetadata(indexMetadata);
        engine.onSettingsChanged(indexSettings.getTranslogRetentionAge(), indexSettings.getTranslogRetentionSize(),
                                 indexSettings.getSoftDeleteRetentionOperations());
        assertThat(engine.getTranslog().stats().getUncommittedOperations()).isEqualTo(1);
        assertThat(engine.shouldPeriodicallyFlush()).isEqualTo(false);
        doc = testParsedDocument(Integer.toString(1), testDocumentWithTextField(), SOURCE);
        engine.index(indexForDoc(doc));
        assertThat(engine.getTranslog().stats().getUncommittedOperations()).isEqualTo(2);
        engine.refresh("test");
        engine.forceMerge(false, 1, false, UUIDs.randomBase64UUID());
        assertBusy(() -> {
            // the merge listner runs concurrently after the force merge returned
            assertThat(engine.shouldPeriodicallyFlush()).isTrue();
        });
        engine.flush();
        assertThat(engine.shouldPeriodicallyFlush()).isFalse();
    }

    @Test
    public void testStressShouldPeriodicallyFlush() throws Exception {
        final long flushThreshold = randomLongBetween(120, 5000);
        final long generationThreshold = randomLongBetween(1000, 5000);
        final IndexSettings indexSettings = engine.config().getIndexSettings();
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(Settings.builder().put(indexSettings.getSettings())
                          .put(IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.getKey(), generationThreshold + "b")
                          .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), flushThreshold + "b")).build();
        indexSettings.updateIndexMetadata(indexMetadata);
        engine.onSettingsChanged(indexSettings.getTranslogRetentionAge(), indexSettings.getTranslogRetentionSize(),
            indexSettings.getSoftDeleteRetentionOperations());
        final int numOps = scaledRandomIntBetween(100, 10_000);
        for (int i = 0; i < numOps; i++) {
            final long localCheckPoint = engine.getProcessedLocalCheckpoint();
            final long seqno = randomLongBetween(Math.max(0, localCheckPoint), localCheckPoint + 5);
            final ParsedDocument doc =
                testParsedDocument(Long.toString(seqno), testDocumentWithTextField(), SOURCE);
            engine.index(replicaIndexForDoc(doc, 1L, seqno, false));
            if (rarely() && engine.getTranslog().shouldRollGeneration()) {
                engine.rollTranslogGeneration();
            }
            if (rarely() || engine.shouldPeriodicallyFlush()) {
                engine.flush();
                assertThat(engine.shouldPeriodicallyFlush()).isFalse();
            }
        }
    }

    @Test
    public void testStressUpdateSameDocWhileGettingIt() throws IOException, InterruptedException {
        final int iters = randomIntBetween(1, 15);
        for (int i = 0; i < iters; i++) {
            // this is a reproduction of https://github.com/elastic/elasticsearch/issues/28714
            try (Store store = createStore(); InternalEngine engine = createEngine(store, createTempDir())) {
                final IndexSettings indexSettings = engine.config().getIndexSettings();
                final IndexMetadata indexMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
                    .settings(Settings.builder().put(indexSettings.getSettings())
                                  .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), TimeValue.timeValueMillis(1))).build();
                engine.engineConfig.getIndexSettings().updateIndexMetadata(indexMetadata);
                engine.onSettingsChanged(indexSettings.getTranslogRetentionAge(), indexSettings.getTranslogRetentionSize(),
                    indexSettings.getSoftDeleteRetentionOperations());
                ParsedDocument document = testParsedDocument(Integer.toString(0), testDocumentWithTextField(), SOURCE);
                final Engine.Index doc = new Engine.Index(newUid(document), document, UNASSIGNED_SEQ_NO, 0,
                    Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(),
                    -1, false, UNASSIGNED_SEQ_NO, 0);
                // first index an append only document and then delete it. such that we have it in the tombstones
                engine.index(doc);
                engine.delete(new Engine.Delete(
                    doc.id(),
                    doc.uid(),
                    UNASSIGNED_SEQ_NO,
                    primaryTerm.get(),
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    System.nanoTime(),
                    UNASSIGNED_SEQ_NO,
                    0
                ));

                // now index more append only docs and refresh so we re-enabel the optimization for unsafe version map
                ParsedDocument document1 = testParsedDocument(Integer.toString(1), testDocumentWithTextField(), SOURCE);
                engine.index(new Engine.Index(newUid(document1), document1, UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false,
                    UNASSIGNED_SEQ_NO, 0));
                engine.refresh("test");
                ParsedDocument document2 = testParsedDocument(Integer.toString(2), testDocumentWithTextField(), SOURCE);
                engine.index(new Engine.Index(newUid(document2), document2, UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false,
                    UNASSIGNED_SEQ_NO, 0));
                engine.refresh("test");
                ParsedDocument document3 = testParsedDocument(Integer.toString(3), testDocumentWithTextField(), SOURCE);
                final Engine.Index doc3 = new Engine.Index(newUid(document3), document3, UNASSIGNED_SEQ_NO, 0,
                    Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(),
                    -1, false, UNASSIGNED_SEQ_NO, 0);
                engine.index(doc3);
                engine.engineConfig.setEnableGcDeletes(true);
                // once we are here the version map is unsafe again and we need to do a refresh inside the get calls to ensure we
                // de-optimize. We also enabled GCDeletes which now causes pruning tombstones inside that refresh that is done internally
                // to ensure we de-optimize. One get call will purne and the other will try to lock the version map concurrently while
                // holding the lock that pruneTombstones needs and we have a deadlock
                CountDownLatch awaitStarted = new CountDownLatch(1);
                Thread thread = new Thread(() -> {
                    awaitStarted.countDown();
                    try (Engine.GetResult getResult = engine.get(new Engine.Get(
                        doc3.id(), doc3.uid()), engine::acquireSearcher)) {

                        assertThat(getResult.docIdAndVersion()).isNotNull();
                    }
                });
                thread.start();
                awaitStarted.await();
                try (Engine.GetResult getResult = engine.get(
                    new Engine.Get(doc.id(), doc.uid()),
                    engine::acquireSearcher)) {

                    assertThat(getResult.docIdAndVersion()).isNull();
                }
                thread.join();
            }
        }
    }

    @Test
    public void testPruneOnlyDeletesAtMostLocalCheckpoint() throws Exception {
        final AtomicLong clock = new AtomicLong(0);
        threadPool = spy(threadPool);
        when(threadPool.relativeTimeInMillis()).thenAnswer(invocation -> clock.get());
        final long gcInterval = randomIntBetween(0, 10);
        final IndexSettings indexSettings = engine.config().getIndexSettings();
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(Settings.builder().put(indexSettings.getSettings())
                          .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), TimeValue.timeValueMillis(gcInterval).getStringRep())).build();
        indexSettings.updateIndexMetadata(indexMetadata);
        try (Store store = createStore();
             InternalEngine engine = createEngine(store, createTempDir())) {
            engine.config().setEnableGcDeletes(false);
            for (int i = 0, docs = scaledRandomIntBetween(0, 10); i < docs; i++) {
                index(engine, i);
            }
            final long deleteBatch = between(10, 20);
            final long gapSeqNo = randomLongBetween(
                engine.getSeqNoStats(-1).getMaxSeqNo() + 1, engine.getSeqNoStats(-1).getMaxSeqNo() + deleteBatch);
            for (int i = 0; i < deleteBatch; i++) {
                final long seqno = generateNewSeqNo(engine);
                if (seqno != gapSeqNo) {
                    if (randomBoolean()) {
                        clock.incrementAndGet();
                    }
                    engine.delete(replicaDeleteForDoc(UUIDs.randomBase64UUID(), 1, seqno, threadPool.relativeTimeInMillis()));
                }
            }

            List<DeleteVersionValue> tombstones = new ArrayList<>(tombstonesInVersionMap(engine).values());
            engine.config().setEnableGcDeletes(true);
            // Prune tombstones whose seqno < gap_seqno and timestamp < clock-gcInterval.
            clock.set(randomLongBetween(gcInterval, deleteBatch + gcInterval));
            engine.refresh("test");
            tombstones.removeIf(v -> v.seqNo < gapSeqNo && v.time < clock.get() - gcInterval);
            assertThat(tombstonesInVersionMap(engine).values()).containsExactlyInAnyOrder(tombstones.toArray(new DeleteVersionValue[]{}));
            // Prune tombstones whose seqno at most the local checkpoint (eg. seqno < gap_seqno).
            clock.set(randomLongBetween(deleteBatch + gcInterval * 4/3, 100)); // Need a margin for gcInterval/4.
            engine.refresh("test");
            tombstones.removeIf(v -> v.seqNo < gapSeqNo);
            assertThat(tombstonesInVersionMap(engine).values()).containsExactlyInAnyOrder(tombstones.toArray(new DeleteVersionValue[]{}));
            // Fill the seqno gap - should prune all tombstones.
            clock.set(between(0, 100));
            if (randomBoolean()) {
                engine.index(replicaIndexForDoc(testParsedDocument("d", testDocumentWithTextField(),
                                                                   SOURCE), 1, gapSeqNo, false));
            } else {
                engine.delete(replicaDeleteForDoc(UUIDs.randomBase64UUID(), Versions.MATCH_ANY,
                                                  gapSeqNo, threadPool.relativeTimeInMillis()));
            }
            clock.set(randomLongBetween(100 + gcInterval * 4/3, Long.MAX_VALUE)); // Need a margin for gcInterval/4.
            engine.refresh("test");
            assertThat(tombstonesInVersionMap(engine).values()).isEmpty();
        }
    }

    @Test
    public void testTrimUnsafeCommits() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final int maxSeqNo = 40;
        final List<Long> seqNos = LongStream.rangeClosed(0, maxSeqNo).boxed().collect(Collectors.toList());
        Collections.shuffle(seqNos, random());
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(),
                                         null, null, globalCheckpoint::get);
            final List<Long> commitMaxSeqNo = new ArrayList<>();
            final long minTranslogGen;
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < seqNos.size(); i++) {
                    ParsedDocument doc = testParsedDocument(Long.toString(seqNos.get(i)), testDocument(),
                                                            new BytesArray("{}"));
                    Engine.Index index = new Engine.Index(newUid(doc), doc, seqNos.get(i), 0,
                                                          1, null, REPLICA, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0);
                    engine.index(index);
                    if (randomBoolean()) {
                        engine.flush();
                        final Long maxSeqNoInCommit = seqNos.subList(0, i + 1).stream().max(Long::compareTo).orElse(-1L);
                        commitMaxSeqNo.add(maxSeqNoInCommit);
                    }
                }
                globalCheckpoint.set(randomInt(maxSeqNo));
                engine.syncTranslog();
                minTranslogGen = engine.getTranslog().getMinFileGeneration();
            }

            store.trimUnsafeCommits(globalCheckpoint.get(), minTranslogGen,config.getIndexSettings().getIndexVersionCreated());
            long safeMaxSeqNo =
                commitMaxSeqNo.stream().filter(s -> s <= globalCheckpoint.get())
                    .reduce((s1, s2) -> s2) // get the last one.
                    .orElse(SequenceNumbers.NO_OPS_PERFORMED);
            final List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
            assertThat(commits).hasSize(1);
            assertThat(commits.get(0).getUserData().get(SequenceNumbers.MAX_SEQ_NO)).isEqualTo(Long.toString(safeMaxSeqNo));
            try (IndexReader reader = DirectoryReader.open(commits.get(0))) {
                for (LeafReaderContext context: reader.leaves()) {
                    final NumericDocValues values = context.reader().getNumericDocValues(SysColumns.Names.SEQ_NO);
                    if (values != null) {
                        for (int docID = 0; docID < context.reader().maxDoc(); docID++) {
                            if (values.advanceExact(docID) == false) {
                                throw new AssertionError("Document does not have a seq number: " + docID);
                            }
                            assertThat(values.longValue()).isLessThanOrEqualTo(globalCheckpoint.get());
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testLuceneHistoryOnPrimary() throws Exception {
        final List<Engine.Operation> operations = generateSingleDocHistory(
            false, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), 2, 10, 300, "1");
        assertOperationHistoryInLucene(operations);
    }

    @Test
    public void testLuceneHistoryOnReplica() throws Exception {
        final List<Engine.Operation> operations = generateSingleDocHistory(
            true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), 2, 10, 300, "2");
        Randomness.shuffle(operations);
        assertOperationHistoryInLucene(operations);
    }

    private void assertOperationHistoryInLucene(List<Engine.Operation> operations) throws IOException {
        final MergePolicy keepSoftDeleteDocsMP = new SoftDeletesRetentionMergePolicy(
            Lucene.SOFT_DELETES_FIELD, MatchAllDocsQuery::new, engine.config().getMergePolicy());
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), randomLongBetween(0, 10));
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        Set<Long> expectedSeqNos = new HashSet<>();
        try (Store store = createStore();
             Engine engine = createEngine(config(indexSettings, store, createTempDir(), keepSoftDeleteDocsMP, null))) {
            for (Engine.Operation op : operations) {
                if (op instanceof Engine.Index) {
                    Engine.IndexResult indexResult = engine.index((Engine.Index) op);
                    assertThat(indexResult.getFailure()).isNull();
                    expectedSeqNos.add(indexResult.getSeqNo());
                } else {
                    Engine.DeleteResult deleteResult = engine.delete((Engine.Delete) op);
                    assertThat(deleteResult.getFailure()).isNull();
                    expectedSeqNos.add(deleteResult.getSeqNo());
                }
                if (rarely()) {
                    engine.refresh("test");
                }
                if (rarely()) {
                    engine.flush();
                }
                if (rarely()) {
                    engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
                }
            }
            List<Translog.Operation> actualOps = readAllOperationsInLucene(engine);
            assertThat(actualOps.stream().map(o -> o.seqNo()).toList()).containsExactlyInAnyOrder(expectedSeqNos.toArray(new Long[]{}));
            assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
        }
    }

    @Test
    public void testKeepMinRetainedSeqNoByMergePolicy() throws IOException {
        IOUtils.close(engine, store);
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), randomLongBetween(0, 10));
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final AtomicLong retentionLeasesVersion = new AtomicLong();
        final AtomicReference<RetentionLeases> retentionLeasesHolder = new AtomicReference<>(
            new RetentionLeases(primaryTerm, retentionLeasesVersion.get(), Collections.emptyList()));
        final List<Engine.Operation> operations = generateSingleDocHistory(
            true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), 2, 10, 300, "2");
        Randomness.shuffle(operations);
        Set<Long> existingSeqNos = new HashSet<>();
        store = createStore();
        engine = createEngine(config(
            indexSettings,
            store,
            createTempDir(),
            newMergePolicy(),
            null,
            null,
            globalCheckpoint::get,
            retentionLeasesHolder::get
        ));
        assertThat(engine.getMinRetainedSeqNo()).isEqualTo(0L);
        long lastMinRetainedSeqNo = engine.getMinRetainedSeqNo();
        for (Engine.Operation op : operations) {
            final Engine.Result result;
            if (op instanceof Engine.Index) {
                result = engine.index((Engine.Index) op);
            } else {
                result = engine.delete((Engine.Delete) op);
            }
            existingSeqNos.add(result.getSeqNo());
            if (randomBoolean()) {
                engine.syncTranslog(); // advance persisted local checkpoint
                assertThat(engine.getPersistedLocalCheckpoint()).isEqualTo(engine.getProcessedLocalCheckpoint());
                globalCheckpoint.set(
                    randomLongBetween(globalCheckpoint.get(), engine.getLocalCheckpointTracker().getPersistedCheckpoint()));
            }
            if (randomBoolean()) {
                retentionLeasesVersion.incrementAndGet();
                final int length = randomIntBetween(0, 8);
                final List<RetentionLease> leases = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    final String id = randomAlphaOfLength(8);
                    final long retainingSequenceNumber = randomLongBetween(0, Math.max(0, globalCheckpoint.get()));
                    final long timestamp = randomLongBetween(0L, Long.MAX_VALUE);
                    final String source = randomAlphaOfLength(8);
                    leases.add(new RetentionLease(id, retainingSequenceNumber, timestamp, source));
                }
                retentionLeasesHolder.set(new RetentionLeases(primaryTerm, retentionLeasesVersion.get(), leases));
            }
            if (rarely()) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), randomLongBetween(0, 10));
                indexSettings.updateIndexMetadata(IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build());
                engine.onSettingsChanged(indexSettings.getTranslogRetentionAge(), indexSettings.getTranslogRetentionSize(),
                    indexSettings.getSoftDeleteRetentionOperations());
            }
            if (rarely()) {
                engine.refresh("test");
            }
            if (rarely()) {
                engine.flush(true, true);
                assertThat(Long.parseLong(engine.getLastCommittedSegmentInfos().userData.get(Engine.MIN_RETAINED_SEQNO))).isEqualTo(engine.getMinRetainedSeqNo());
            }
            if (rarely()) {
                engine.forceMerge(randomBoolean(), 1, false, UUIDs.randomBase64UUID());
            }
            try (Closeable ignored = engine.acquireHistoryRetentionLock(Engine.HistorySource.INDEX)) {
                long minRetainSeqNos = engine.getMinRetainedSeqNo();
                assertThat(minRetainSeqNos).isLessThanOrEqualTo(globalCheckpoint.get() + 1);
                Long[] expectedOps = existingSeqNos.stream().filter(seqno -> seqno >= minRetainSeqNos).toArray(Long[]::new);
                Set<Long> actualOps = readAllOperationsInLucene(engine).stream()
                    .map(Translog.Operation::seqNo).collect(Collectors.toSet());
                assertThat(actualOps).containsExactlyInAnyOrder(expectedOps);
            }
            try (Engine.IndexCommitRef commitRef = engine.acquireSafeIndexCommit()) {
                IndexCommit safeCommit = commitRef.getIndexCommit();
                if (safeCommit.getUserData().containsKey(Engine.MIN_RETAINED_SEQNO)) {
                    lastMinRetainedSeqNo = Long.parseLong(safeCommit.getUserData().get(Engine.MIN_RETAINED_SEQNO));
                }
            }
        }
        if (randomBoolean()) {
            engine.close();
        } else {
            engine.flushAndClose();
        }
        try (InternalEngine recoveringEngine = new InternalEngine(engine.config())) {
            assertThat(recoveringEngine.getMinRetainedSeqNo()).isEqualTo(lastMinRetainedSeqNo);
        }
    }

    @Test
    public void testLastRefreshCheckpoint() throws Exception {
        AtomicBoolean done = new AtomicBoolean();
        Thread[] refreshThreads = new Thread[between(1, 8)];
        CountDownLatch latch = new CountDownLatch(refreshThreads.length);
        for (int i = 0; i < refreshThreads.length; i++) {
            latch.countDown();
            refreshThreads[i] = new Thread(() -> {
                while (done.get() == false) {
                    long checkPointBeforeRefresh = engine.getProcessedLocalCheckpoint();
                    engine.refresh("test", randomFrom(Engine.SearcherScope.values()), true);
                    assertThat(engine.lastRefreshedCheckpoint()).isGreaterThanOrEqualTo(checkPointBeforeRefresh);
                }
            });
            refreshThreads[i].start();
        }
        latch.await();
        List<Engine.Operation> ops = generateSingleDocHistory(
            true, VersionType.EXTERNAL, 1, 10, 1000, "1");
        concurrentlyApplyOps(ops, engine);
        done.set(true);
        for (Thread thread : refreshThreads) {
            thread.join();
        }
        engine.refresh("test");
        assertThat(engine.lastRefreshedCheckpoint()).isEqualTo(engine.getProcessedLocalCheckpoint());
    }

    @Test
    public void testLuceneSnapshotRefreshesOnlyOnce() throws Exception {
        final long maxSeqNo = randomLongBetween(10, 50);
        final AtomicLong refreshCounter = new AtomicLong();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(Settings.builder().
                put(defaultSettings.getSettings()).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)).build());
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), newMergePolicy(),
                                                         null,
                                                         new ReferenceManager.RefreshListener() {
                                                             @Override
                                                             public void beforeRefresh() {
                                                                 refreshCounter.incrementAndGet();
                                                             }

                                                             @Override
                                                             public void afterRefresh(boolean didRefresh) {
                                                             }
                                                         }, () -> SequenceNumbers.NO_OPS_PERFORMED))) {
            for (long seqNo = 0; seqNo <= maxSeqNo; seqNo++) {
                final ParsedDocument doc = testParsedDocument("id_" + seqNo, testDocumentWithTextField("test"),
                                                              new BytesArray("{}".getBytes(Charset.defaultCharset())));
                engine.index(replicaIndexForDoc(doc, 1, seqNo, randomBoolean()));
            }

            final long initialRefreshCount = refreshCounter.get();
            final Thread[] snapshotThreads = new Thread[between(1, 3)];
            CountDownLatch latch = new CountDownLatch(1);
            for (int i = 0; i < snapshotThreads.length; i++) {
                final long min = randomLongBetween(0, maxSeqNo - 5);
                final long max = randomLongBetween(min, maxSeqNo);
                snapshotThreads[i] = new Thread(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError(e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        latch.await();
                        Translog.Snapshot changes = engine.newChangesSnapshot("test", min, max, true);
                        changes.close();
                    }
                });
                snapshotThreads[i].start();
            }
            latch.countDown();
            for (Thread thread : snapshotThreads) {
                thread.join();
            }
            assertThat(refreshCounter.get()).isEqualTo(initialRefreshCount + 1L);
            assertThat(engine.lastRefreshedCheckpoint()).isEqualTo(maxSeqNo);
        }
    }

    @Test
    public void testAcquireSearcherOnClosingEngine() throws Exception {
        engine.close();
        assertThatThrownBy(() -> engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL))
            .isExactlyInstanceOf(AlreadyClosedException.class);
    }

    @Test
    public void testNoOpOnClosingEngine() throws Exception {
        engine.close();
        Settings settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build());
        assertThat(indexSettings.isSoftDeleteEnabled()).isTrue();
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null))) {
            engine.close();
            assertThatThrownBy(() -> engine.noOp(
                new Engine.NoOp(2, primaryTerm.get(), LOCAL_TRANSLOG_RECOVERY, System.nanoTime(), "reason"))
            ).isExactlyInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testSoftDeleteOnClosingEngine() throws Exception {
        engine.close();
        Settings settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build());
        assertThat(indexSettings.isSoftDeleteEnabled()).isTrue();
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null))) {
            engine.close();
            assertThatThrownBy(() -> engine.delete(replicaDeleteForDoc("test", 42, 7, System.nanoTime())))
                .isExactlyInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testTrackMaxSeqNoOfUpdatesOrDeletesOnPrimary() throws Exception {
        engine.close();
        Set<String> liveDocIds = new HashSet<>();
        engine = new InternalEngine(engine.config());
        assertThat(engine.getMaxSeqNoOfUpdatesOrDeletes()).isEqualTo(-1L);
        int numOps = between(1, 500);
        for (int i = 0; i < numOps; i++) {
            long currentMaxSeqNoOfUpdates = engine.getMaxSeqNoOfUpdatesOrDeletes();
            ParsedDocument doc = createParsedDoc(Integer.toString(between(1, 100)));
            if (randomBoolean()) {
                Engine.IndexResult result = engine.index(indexForDoc(doc));
                if (liveDocIds.add(doc.id()) == false) {
                    assertThat(engine.getMaxSeqNoOfUpdatesOrDeletes()).as("update operations on primary must advance max_seq_no_of_updates").isEqualTo(Math.max(currentMaxSeqNoOfUpdates, result.getSeqNo()));
                } else {
                    assertThat(engine.getMaxSeqNoOfUpdatesOrDeletes()).as("append operations should not advance max_seq_no_of_updates").isEqualTo(currentMaxSeqNoOfUpdates);
                }
            } else {
                Engine.DeleteResult result = engine.delete(new Engine.Delete(
                    doc.id(),
                    newUid(doc.id()),
                    UNASSIGNED_SEQ_NO,
                    primaryTerm.get(),
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    System.nanoTime(),
                    UNASSIGNED_SEQ_NO,
                    0
                ));
                liveDocIds.remove(doc.id());
                assertThat(engine.getMaxSeqNoOfUpdatesOrDeletes()).as("delete operations on primary must advance max_seq_no_of_updates").isEqualTo(Math.max(currentMaxSeqNoOfUpdates, result.getSeqNo()));
            }
        }
    }

    @Test
    public void testRebuildLocalCheckpointTrackerAndVersionMap() throws Exception {
        Settings.Builder settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 10000)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
        final IndexMetadata indexMetadata = IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetadata);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        Path translogPath = createTempDir();
        List<Engine.Operation> operations = generateHistoryOnReplica(between(1, 500), randomBoolean(), randomBoolean());
        List<List<Engine.Operation>> commits = new ArrayList<>();
        commits.add(new ArrayList<>());
        try (Store store = createStore()) {
            EngineConfig config = config(indexSettings, store, translogPath, NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
            final List<DocIdSeqNoAndSource> docs;
            try (InternalEngine engine = createEngine(config)) {
                List<Engine.Operation> flushedOperations = new ArrayList<>();
                for (Engine.Operation op : operations) {
                    flushedOperations.add(op);
                    applyOperation(engine, op);
                    if (randomBoolean()) {
                        engine.syncTranslog();
                        globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                    }
                    if (randomInt(100) < 10) {
                        engine.refresh("test");
                    }
                    if (randomInt(100) < 5) {
                        engine.flush(true, true);
                        flushedOperations.sort(Comparator.comparing(Engine.Operation::seqNo));
                        commits.add(new ArrayList<>(flushedOperations));
                    }
                }
                docs = getDocIds(engine, true);
            }
            List<Engine.Operation> operationsInSafeCommit = null;
            for (int i = commits.size() - 1; i >= 0; i--) {
                if (commits.get(i).stream().allMatch(op -> op.seqNo() <= globalCheckpoint.get())) {
                    operationsInSafeCommit = commits.get(i);
                    break;
                }
            }
            assertThat(operationsInSafeCommit).isNotNull();
            try (InternalEngine engine = new InternalEngine(config)) { // do not recover from translog
                final Map<BytesRef, Engine.Operation> deletesAfterCheckpoint = new HashMap<>();
                for (Engine.Operation op : operationsInSafeCommit) {
                    if (op instanceof Engine.NoOp == false && op.seqNo() > engine.getPersistedLocalCheckpoint()) {
                        deletesAfterCheckpoint.put(new Term(SysColumns.Names.ID, Uid.encodeId(op.id())).bytes(), op);
                    }
                }
                deletesAfterCheckpoint.values().removeIf(o -> o instanceof Engine.Delete == false);
                final Map<BytesRef, VersionValue> versionMap = engine.getVersionMap();
                for (BytesRef uid : deletesAfterCheckpoint.keySet()) {
                    final VersionValue versionValue = versionMap.get(uid);
                    final Engine.Operation op = deletesAfterCheckpoint.get(uid);
                    final String msg = versionValue + " vs " +
                        "op[" + op.operationType() + "id=" + op.id() + " seqno=" + op.seqNo() + " term=" + op.primaryTerm() + "]";
                    assertThat(versionValue).isExactlyInstanceOf(DeleteVersionValue.class);
                    assertThat(versionValue.seqNo).as(msg).isEqualTo(op.seqNo());
                    assertThat(versionValue.term).as(msg).isEqualTo(op.primaryTerm());
                    assertThat(versionValue.version).as(msg).isEqualTo(op.version());
                }
                assertThat(versionMap.keySet()).isEqualTo(deletesAfterCheckpoint.keySet());
                final LocalCheckpointTracker tracker = engine.getLocalCheckpointTracker();
                final Set<Long> seqNosInSafeCommit = operationsInSafeCommit.stream().map(op -> op.seqNo()).collect(Collectors.toSet());
                for (Engine.Operation op : operations) {
                    assertThat(tracker.hasProcessed(op.seqNo())).as(
                        "seq_no=" + op.seqNo() + " max_seq_no=" + tracker.getMaxSeqNo() + " checkpoint=" + tracker.getProcessedCheckpoint()).isEqualTo(seqNosInSafeCommit.contains(op.seqNo()));
                }
                engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                assertThat(getDocIds(engine, true)).isEqualTo(docs);
            }
        }
    }

    @Test
    public void testRequireSoftDeletesWhenAccessingChangesSnapshot() throws Exception {
        try (Store store = createStore()) {
            final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
                IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(Settings.builder().
                    put(defaultSettings.getSettings()).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false)).build());
            try (InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), newMergePolicy(), null))) {
                assertThatThrownBy(() -> engine.newChangesSnapshot("test", 0, randomNonNegativeLong(), randomBoolean()))
                    .isExactlyInstanceOf(AssertionError.class)
                    .hasMessageContaining("does not have soft-deletes enabled");
            }
        }
    }

    private void assertLuceneOperations(InternalEngine engine,
                                        long expectedAppends,
                                        long expectedUpdates,
                                        long expectedDeletes) {
        String message = "Lucene operations mismatched;" +
                         " appends [actual:" + engine.getNumDocAppends() + ", expected:" + expectedAppends + "]," +
                         " updates [actual:" + engine.getNumDocUpdates() + ", expected:" + expectedUpdates + "]," +
                         " deletes [actual:" + engine.getNumDocDeletes() + ", expected:" + expectedDeletes + "]";
        assertThat(engine.getNumDocAppends()).as(message).isEqualTo(expectedAppends);
        assertThat(engine.getNumDocUpdates()).as(message).isEqualTo(expectedUpdates);
        assertThat(engine.getNumDocDeletes()).as(message).isEqualTo(expectedDeletes);
    }

    @Test
    public void testStoreHonorsLuceneVersion() throws IOException {
        for (Version createdVersion : Arrays.asList(
            Version.CURRENT, VersionUtils.getPreviousMinorVersion(), Version.CURRENT.minimumIndexCompatibilityVersion())) {
            Settings settings = Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_VERSION_CREATED, createdVersion).build();
            IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
            try (Store store = createStore();
                    InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null))) {
                ParsedDocument doc = testParsedDocument("1", new Document(),
                        new BytesArray("{}".getBytes("UTF-8")));
                engine.index(appendOnlyPrimary(doc, false, 1));
                engine.refresh("test");
                try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
                    LeafReader leafReader = getOnlyLeafReader(searcher.getIndexReader());
                    assertThat(leafReader.getMetaData().getCreatedVersionMajor()).isEqualTo(createdVersion.luceneVersion.major);
                }
            }
        }
    }

    @Test
    public void testMaxSeqNoInCommitUserData() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        Thread rollTranslog = new Thread(() -> {
            while (running.get() && engine.getTranslog().currentFileGeneration() < 500) {
                engine.rollTranslogGeneration(); // make adding operations to translog slower
            }
        });
        rollTranslog.start();

        Thread indexing = new Thread(() -> {
            long seqNo = 0;
            while (running.get() && seqNo <= 1000) {
                try {
                    String id = Long.toString(between(1, 50));
                    if (randomBoolean()) {
                        ParsedDocument doc = testParsedDocument(id, testDocumentWithTextField(), SOURCE);
                        engine.index(replicaIndexForDoc(doc, 1L, seqNo, false));
                    } else {
                        engine.delete(replicaDeleteForDoc(id, 1L, seqNo, 0L));
                    }
                    seqNo++;
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        });
        indexing.start();

        int numCommits = between(5, 20);
        for (int i = 0; i < numCommits; i++) {
            engine.flush(false, true);
        }
        running.set(false);
        indexing.join();
        rollTranslog.join();
        assertMaxSeqNoInCommitUserData(engine);
    }

    @Test
    public void testPruneAwayDeletedButRetainedIds() throws Exception {
        IOUtils.close(engine, store);
        Settings settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build());
        store = createStore(indexSettings, newDirectory());
        LogDocMergePolicy policy = new LogDocMergePolicy();
        policy.setMinMergeDocs(10000);
        try (InternalEngine engine = createEngine(indexSettings, store, createTempDir(), policy)) {
            int numDocs = between(1, 20);
            logger.info("" + numDocs);
            for (int i = 0; i < numDocs; i++) {
                index(engine, i);
            }
            engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
            engine.delete(new Engine.Delete("0", newUid("0"), primaryTerm.get()));
            engine.refresh("test");
            // now we have 2 segments since we now added a tombstone plus the old segment with the delete
            try (Searcher searcher = engine.acquireSearcher("test")) {
                IndexReader reader = searcher.getIndexReader();
                assertThat(reader.leaves().size()).isEqualTo(2);
                LeafReaderContext leafReaderContext = reader.leaves().get(0);
                LeafReader leafReader = leafReaderContext.reader();
                assertThat(leafReader.numDeletedDocs()).as("the delete and the tombstone").isEqualTo(1);
                assertThat(leafReader.maxDoc()).isEqualTo(numDocs);
                Terms id = leafReader.terms("_id");
                assertThat(id).isNotNull();
                assertThat(id.size()).as("deleted IDs are NOT YET pruned away").isEqualTo(reader.numDocs() + 1);
                TermsEnum iterator = id.iterator();
                assertThat(iterator.seekExact(Uid.encodeId("0"))).isTrue();
            }

            // lets force merge the tombstone and the original segment and make sure the doc is still there but the ID term is gone
            engine.forceMerge(true, 1, false, UUIDs.randomBase64UUID());
            engine.refresh("test");
            try (Searcher searcher = engine.acquireSearcher("test")) {
                IndexReader reader = searcher.getIndexReader();
                assertThat(reader.leaves().size()).isEqualTo(1);
                LeafReaderContext leafReaderContext = reader.leaves().get(0);
                LeafReader leafReader = leafReaderContext.reader();
                assertThat(leafReader.numDeletedDocs()).as("the delete and the tombstone").isEqualTo(2);
                assertThat(leafReader.maxDoc()).isEqualTo(numDocs + 1);
                Terms id = leafReader.terms("_id");
                if (numDocs == 1) {
                    assertThat(id).isNull(); // everything is pruned away
                    assertThat(leafReader.numDocs()).isEqualTo(0);
                } else {
                    assertThat(id).isNotNull();
                    assertThat(id.size()).as("deleted IDs are pruned away").isEqualTo(reader.numDocs());
                    TermsEnum iterator = id.iterator();
                    assertThat(iterator.seekExact(Uid.encodeId("0"))).isFalse();
                }
             }
         }
    }

    @Test
    public void testRecoverFromLocalTranslog() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        Path translogPath = createTempDir();
        List<Engine.Operation> operations = generateHistoryOnReplica(between(1, 500), randomBoolean(), randomBoolean());
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpoint::get);
            final List<DocIdSeqNoAndSource> docs;
            try (InternalEngine engine = createEngine(config)) {
                for (Engine.Operation op : operations) {
                    applyOperation(engine, op);
                    if (randomBoolean()) {
                        engine.syncTranslog();
                        globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                    }
                    if (randomInt(100) < 10) {
                        engine.refresh("test");
                    }
                    if (randomInt(100) < 5) {
                        engine.flush();
                    }
                    if (randomInt(100) < 5) {
                        engine.forceMerge(randomBoolean(), 1, false, UUIDs.randomBase64UUID());
                    }
                }
                if (randomBoolean()) {
                    // engine is flushed properly before shutting down.
                    engine.syncTranslog();
                    globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                    engine.flush();
                }
                docs = getDocIds(engine, true);
            }
            try (InternalEngine engine = new InternalEngine(config)) {
                engine.onSettingsChanged(TimeValue.MINUS_ONE, ByteSizeValue.ZERO, 0);
                engine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                assertThat(getDocIds(engine, randomBoolean())).isEqualTo(docs);
                if (engine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo() == globalCheckpoint.get()) {
                    assertThat(engine.getTranslog().getMinFileGeneration()).as("engine should trim all unreferenced translog after recovery").isEqualTo(engine.getTranslog().currentFileGeneration());
                }
            }
        }
    }


    private Map<BytesRef, DeleteVersionValue> tombstonesInVersionMap(InternalEngine engine) {
        return engine.getVersionMap().entrySet().stream()
            .filter(e -> e.getValue() instanceof DeleteVersionValue)
            .collect(Collectors.toMap(e -> e.getKey(), e -> (DeleteVersionValue) e.getValue()));
    }

    @Test
    public void testTreatDocumentFailureAsFatalError() throws Exception {
        AtomicReference<IOException> addDocException = new AtomicReference<>();
        IndexWriterFactory indexWriterFactory = (dir, iwc) -> new IndexWriter(dir, iwc) {
            @Override
            public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
                final IOException ex = addDocException.getAndSet(null);
                if (ex != null) {
                    throw ex;
                }
                return super.addDocument(doc);
            }
        };
        try (Store store = createStore();
             InternalEngine engine = createEngine(defaultSettings,
                                                  store,
                                                  createTempDir(),
                                                  NoMergePolicy.INSTANCE,
                                                  indexWriterFactory)) {
            final ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField(), SOURCE);
            Engine.Operation.Origin origin = randomFrom(REPLICA, LOCAL_RESET, PEER_RECOVERY);
            Engine.Index index = new Engine.Index(
                newUid(doc),
                doc,
                randomNonNegativeLong(),
                primaryTerm.get(),
                randomNonNegativeLong(),
                null,
                origin,
                System.nanoTime(),
                -1,
                false,
                UNASSIGNED_SEQ_NO,
                UNASSIGNED_PRIMARY_TERM);
            addDocException.set(new IOException("simulated"));
            assertThatThrownBy(() -> engine.index(index))
                .isExactlyInstanceOf(IOException.class);
            assertThat(engine.isClosed.get()).isTrue();
            assertThat(engine.failedEngine.get()).isNotNull();
        }
    }

    /**
     * We can trim translog on primary promotion and peer recovery based on the fact we add operations with either
     * REPLICA or PEER_RECOVERY origin to translog although they already exist in the engine (i.e. hasProcessed() == true).
     * If we decide not to add those already-processed operations to translog, we need to study carefully the consequence
     * of the translog trimming in these two places.
     */
    @Test
    public void testAlwaysRecordReplicaOrPeerRecoveryOperationsToTranslog() throws Exception {
        List<Engine.Operation> operations = generateHistoryOnReplica(between(1, 100), randomBoolean(), randomBoolean());
        applyOperations(engine, operations);
        Set<Long> seqNos = operations.stream().map(Engine.Operation::seqNo).collect(Collectors.toSet());
        try (Translog.Snapshot snapshot = getTranslog(engine).newSnapshot()) {
            assertThat(snapshot.totalOperations()).isEqualTo(operations.size());
            assertThat(TestTranslog.drainSnapshot(snapshot, false).stream().map(Translog.Operation::seqNo).collect(Collectors.toSet())).isEqualTo(seqNos);
        }
        primaryTerm.set(randomLongBetween(primaryTerm.get(), Long.MAX_VALUE));
        engine.rollTranslogGeneration();
        engine.trimOperationsFromTranslog(primaryTerm.get(), NO_OPS_PERFORMED); // trim everything in translog
        try (Translog.Snapshot snapshot = getTranslog(engine).newSnapshot()) {
            assertThat(snapshot.totalOperations()).isEqualTo(0);
            assertThat(snapshot.next()).isNull();
        }
        applyOperations(engine, operations);
        try (Translog.Snapshot snapshot = getTranslog(engine).newSnapshot()) {
            assertThat(snapshot.totalOperations()).isEqualTo(operations.size());
            assertThat(TestTranslog.drainSnapshot(snapshot, false).stream().map(Translog.Operation::seqNo).collect(Collectors.toSet())).isEqualTo(seqNos);
        }
    }

    @Test
    public void testNoOpFailure() throws IOException {
        engine.close();
        final Settings settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build());
        try (Store store = createStore();
             Engine engine = createEngine((dir, iwc) -> new IndexWriter(dir, iwc) {

                 @Override
                 public long addDocument(Iterable<? extends IndexableField> doc) {
                     throw new IllegalArgumentException("fatal");
                 }

             }, null, null, config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null))) {
            final Engine.NoOp op = new Engine.NoOp(0, 0, PRIMARY, System.currentTimeMillis(), "test");
            assertThatThrownBy(() -> engine.noOp(op))
                .isExactlyInstanceOf(IllegalArgumentException. class)
                .hasMessage("fatal");
            assertThat(engine.isClosed.get()).isTrue();
            assertThat(engine.failedEngine.get()).isNotNull();
            assertThat(engine.failedEngine.get()).isExactlyInstanceOf(IllegalArgumentException.class);
            assertThat(engine.failedEngine.get().getMessage()).isEqualTo("fatal");
        }
    }

    @Test
    public void testDeleteFailureSoftDeletesEnabledDocAlreadyDeleted() throws IOException {
        runTestDeleteFailure(true, InternalEngine::delete);
    }

    @Test
    public void testDeleteFailureSoftDeletesEnabled() throws IOException {
        runTestDeleteFailure(true, (engine, op) -> {});
    }

    @Test
    public void testDeleteFailureSoftDeletesDisabled() throws IOException {
        runTestDeleteFailure(false, (engine, op) -> {});
    }

    private void runTestDeleteFailure(
        final boolean softDeletesEnabled,
        final CheckedBiConsumer<InternalEngine, Engine.Delete, IOException> consumer) throws IOException {
        engine.close();
        final Settings settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), softDeletesEnabled).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build());
        final AtomicReference<ThrowingIndexWriter> iw = new AtomicReference<>();
        try (Store store = createStore();
             InternalEngine engine = createEngine(
                 (dir, iwc) -> {
                     iw.set(new ThrowingIndexWriter(dir, iwc));
                     return iw.get();
                 },
                 null,
                 null,
                 config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null)
            )) {

            engine.index(new Engine.Index(newUid("0"), primaryTerm.get(), InternalEngineTests.createParsedDoc("0")));
            final Engine.Delete op = new Engine.Delete("0", newUid("0"), primaryTerm.get());
            consumer.accept(engine, op);
            iw.get().setThrowFailure(() -> new IllegalArgumentException("fatal"));
            assertThatThrownBy(() -> engine.delete(op))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("fatal");
            assertThat(engine.isClosed.get()).isTrue();
            assertThat(engine.failedEngine.get()).isNotNull();
            assertThat(engine.failedEngine.get()).isExactlyInstanceOf(IllegalArgumentException.class);
            assertThat(engine.failedEngine.get().getMessage()).isEqualTo("fatal");
        }
    }

    @Test
    public void testIndexThrottling() throws Exception {
        final Engine.Index indexWithThrottlingCheck = spy(indexForDoc(createParsedDoc("1", randomBoolean())));
        final Engine.Index indexWithoutThrottlingCheck = spy(indexForDoc(createParsedDoc("2", randomBoolean())));
        doAnswer(invocation -> {
            try {
                assertThat(engine.throttleLockIsHeldByCurrentThread()).isTrue();
                return invocation.callRealMethod();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }).when(indexWithThrottlingCheck).startTime();
        doAnswer(invocation -> {
            try {
                assertThat(engine.throttleLockIsHeldByCurrentThread()).isFalse();
                return invocation.callRealMethod();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }).when(indexWithoutThrottlingCheck).startTime();
        engine.activateThrottling();
        engine.index(indexWithThrottlingCheck);
        engine.deactivateThrottling();
        engine.index(indexWithoutThrottlingCheck);
        verify(indexWithThrottlingCheck, atLeastOnce()).startTime();
        verify(indexWithoutThrottlingCheck, atLeastOnce()).startTime();
    }

    @Test
    public void test_deletes_are_throttled() throws Exception {
        Engine.Delete delete = spy(new Engine.Delete("1", newUid("1"), 1));
        doAnswer(invocation -> {
            try {
                assertThat(engine.throttleLockIsHeldByCurrentThread()).isTrue();
                return invocation.callRealMethod();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }).when(delete).startTime();
        engine.activateThrottling();
        engine.delete(delete);
        engine.deactivateThrottling();
    }

    @Test
    public void testRealtimeGetOnlyRefreshIfNeeded() throws Exception {
        final AtomicInteger refreshCount = new AtomicInteger();
        final ReferenceManager.RefreshListener refreshListener = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {

            }

            @Override
            public void afterRefresh(boolean didRefresh) {
                if (didRefresh) {
                    refreshCount.incrementAndGet();
                }
            }
        };
        try (Store store = createStore()) {
            final EngineConfig config = config(
                defaultSettings,
                store,
                createTempDir(),
                newMergePolicy(),
                null,
                refreshListener,
                null,
                null
            );
            try (InternalEngine engine = createEngine(config)) {
                int numDocs = randomIntBetween(10, 100);
                Set<String> ids = new HashSet<>();
                for (int i = 0; i < numDocs; i++) {
                    String id = Integer.toString(i);
                    engine.index(indexForDoc(createParsedDoc(id)));
                    ids.add(id);
                }
                final int refreshCountBeforeGet = refreshCount.get();
                Thread[] getters = new Thread[randomIntBetween(1, 4)];
                Phaser phaser = new Phaser(getters.length + 1);
                for (int t = 0; t < getters.length; t++) {
                    getters[t] = new Thread(() -> {
                        phaser.arriveAndAwaitAdvance();
                        int iters = randomIntBetween(1, 10);
                        for (int i = 0; i < iters; i++) {
                            ParsedDocument doc = createParsedDoc(randomFrom(ids));
                            try (Engine.GetResult getResult = engine.get(newGet(doc), engine::acquireSearcher)) {
                                assertThat(getResult.docIdAndVersion()).isNotNull();
                            }
                        }
                    });
                    getters[t].start();
                }
                phaser.arriveAndAwaitAdvance();
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc("more-" + i)));
                }
                for (Thread getter : getters) {
                    getter.join();
                }
                assertThat(refreshCount.get()).isLessThanOrEqualTo(refreshCountBeforeGet + 1);
            }
        }
    }

    @Test
    public void testRefreshDoesNotBlockClosing() throws Exception {
        final CountDownLatch refreshStarted = new CountDownLatch(1);
        final CountDownLatch engineClosed = new CountDownLatch(1);
        final ReferenceManager.RefreshListener refreshListener = new ReferenceManager.RefreshListener() {

            @Override
            public void beforeRefresh() {
                refreshStarted.countDown();
                try {
                    engineClosed.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }

            @Override
            public void afterRefresh(boolean didRefresh) {
                assertThat(didRefresh).isFalse();
            }
        };
        try (Store store = createStore()) {
            final EngineConfig config = config(
                defaultSettings,
                store,
                createTempDir(),
                newMergePolicy(),
                null,
                refreshListener,
                null,
                null
            );
            try (InternalEngine engine = createEngine(config)) {
                if (randomBoolean()) {
                    engine.index(indexForDoc(createParsedDoc("id")));
                }
                threadPool.executor(ThreadPool.Names.REFRESH).execute(
                    () -> assertThatThrownBy(
                            () -> engine.refresh("test", randomFrom(Engine.SearcherScope.values()), true)
                        ).isExactlyInstanceOf(AlreadyClosedException.class)
                );
                refreshStarted.await();
                engine.close();
                engineClosed.countDown();
            }
        }
    }

    @Test
    public void testDeleteDocumentFailuresShouldFailEngine() throws IOException {
        engine.close();

        final Settings settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            IndexMetadata.builder(defaultSettings.getIndexMetadata()).settings(settings).build());
        final AtomicReference<ThrowingIndexWriter> iw = new AtomicReference<>();
        try (Store store = createStore();
             InternalEngine engine = createEngine(
                 (dir, iwc) -> {
                     iw.set(new ThrowingIndexWriter(dir, iwc));
                     return iw.get();
                 },
                 null,
                 null,
                 config(indexSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null))) {

            engine.index(new Engine.Index(
                newUid("0"), InternalEngineTests.createParsedDoc("0"), UNASSIGNED_SEQ_NO, primaryTerm.get(),
                Versions.MATCH_DELETED, VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false, UNASSIGNED_SEQ_NO, 0));

            Engine.Delete op = new Engine.Delete(
                "0",
                newUid("0"),
                UNASSIGNED_SEQ_NO,
                primaryTerm.get(),
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                UNASSIGNED_SEQ_NO,
                0
            );

            iw.get().setThrowFailure(() -> new IllegalArgumentException("fatal"));
            assertThatThrownBy(() -> engine.delete(op))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("fatal");
            assertThat(engine.isClosed.get()).isTrue();
            assertThat(engine.failedEngine.get()).isNotNull();
            assertThat(engine.failedEngine.get()).isExactlyInstanceOf(IllegalArgumentException.class);
            assertThat(engine.failedEngine.get().getMessage()).isEqualTo("fatal");
        }
    }

    public static <T> void assertThatIfAssertionEnabled(T actual, Consumer<? super T> matcher) {
        if (InternalEngineTests.class.desiredAssertionStatus()) {
            assertThat(actual).satisfies(matcher);
        }
    }

    @Test
    public void testProducesStoredFieldsReader() throws Exception {
        // Make sure that the engine produces a SequentialStoredFieldsLeafReader.
        // This is required for optimizations on SourceLookup to work, which is in-turn useful for runtime fields.
        ParsedDocument doc = testParsedDocument("1", testDocumentWithTextField("test"),
                                                new BytesArray("{}".getBytes(Charset.defaultCharset())));
        Engine.Index operation = randomBoolean() ?
            appendOnlyPrimary(doc, false, 1)
            : appendOnlyReplica(doc, false, 1, randomIntBetween(0, 5));
        engine.index(operation);
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            IndexReader reader = searcher.getIndexReader();
            assertThat(reader.leaves().size()).isGreaterThanOrEqualTo(1);
            for (LeafReaderContext context: reader.leaves()) {
                assertThat(context.reader()).isInstanceOf(SequentialStoredFieldsLeafReader.class);
                SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) context.reader();
                assertThat(lf.getSequentialStoredFieldsReader()).isNotNull();
            }
        }
    }

    @Test
    public void testMaxDocsOnPrimary() throws Exception {
        engine.close();
        final boolean softDeleteEnabled = engine.config().getIndexSettings().isSoftDeleteEnabled();
        int maxDocs = randomIntBetween(1, 100);
        setIndexWriterMaxDocs(maxDocs);
        try {
            engine = new InternalTestEngine(engine.config(), maxDocs, LocalCheckpointTracker::new);
            int numDocs = between(maxDocs + 1, maxDocs * 2);
            List<Engine.Operation> operations = new ArrayList<>(numDocs);
            for (int i = 0; i < numDocs; i++) {
                final String id;
                if (softDeleteEnabled == false || randomBoolean()) {
                    id = Integer.toString(randomInt(numDocs));
                    operations.add(indexForDoc(createParsedDoc(id)));
                } else {
                    id = "not_found";
                    operations.add(new Engine.Delete(id, newUid(id), primaryTerm.get()));
                }
            }
            for (int i = 0; i < numDocs; i++) {
                final long maxSeqNo = engine.getLocalCheckpointTracker().getMaxSeqNo();
                final Engine.Result result = applyOperation(engine, operations.get(i));
                if (i < maxDocs) {
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.SUCCESS);
                    assertThat(result.getFailure()).isNull();
                    assertThat(engine.getLocalCheckpointTracker().getMaxSeqNo()).isEqualTo(maxSeqNo + 1L);
                } else {
                    assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.FAILURE);
                    assertThat(result.getFailure()).isNotNull();
                    assertThat(result.getFailure().getMessage())
                        .contains("Number of documents in the index can't exceed [" + maxDocs + "]");
                    assertThat(result.getSeqNo()).isEqualTo(UNASSIGNED_SEQ_NO);
                    assertThat(engine.getLocalCheckpointTracker().getMaxSeqNo()).isEqualTo(maxSeqNo);
                }
                assertThat(engine.isClosed.get()).isFalse();
            }
        } finally {
            restoreIndexWriterMaxDocs();
        }
    }

    @Test
    public void testMaxDocsOnReplica() throws Exception {
        assumeTrue("Deletes do not add documents to Lucene with soft-deletes disabled",
            engine.config().getIndexSettings().isSoftDeleteEnabled());
        engine.close();
        int maxDocs = randomIntBetween(1, 100);
        setIndexWriterMaxDocs(maxDocs);
        try {
            engine = new InternalTestEngine(engine.config(), maxDocs, LocalCheckpointTracker::new);
            int numDocs = between(maxDocs + 1, maxDocs * 2);
            List<Engine.Operation> operations = generateHistoryOnReplica(numDocs, randomBoolean(), randomBoolean());
            assertThatThrownBy(() -> {
                for (Engine.Operation op : operations) {
                    applyOperation(engine, op);
                }
            }).isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("number of documents in the index cannot exceed " + maxDocs);
            assertThat(engine.isClosed.get()).isTrue();
        } finally {
            restoreIndexWriterMaxDocs();
        }
    }
}
