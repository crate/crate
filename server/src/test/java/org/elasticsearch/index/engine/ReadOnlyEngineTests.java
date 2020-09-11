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

import io.crate.common.io.IOUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader.getElasticsearchDirectoryReader;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class ReadOnlyEngineTests extends EngineTestCase {

    public void testReadOnlyEngine() throws Exception {
        IOUtils.close(engine, store);
        Engine readOnlyEngine = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            final SeqNoStats lastSeqNoStats;
            final List<DocIdSeqNoAndSource> lastDocIds;
            try (InternalEngine engine = createEngine(config)) {
                Engine.Get get = null;
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                                                  System.nanoTime(), -1, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                    if (get == null || rarely()) {
                        get = newGet(doc);
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                }
                engine.syncTranslog();
                globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                engine.flush();
                readOnlyEngine = new ReadOnlyEngine(engine.engineConfig, engine.getSeqNoStats(globalCheckpoint.get()),
                                                    engine.getTranslogStats(), false, Function.identity());
                lastSeqNoStats = engine.getSeqNoStats(globalCheckpoint.get());
                lastDocIds = getDocIds(engine, true);
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
                for (int i = 0; i < numDocs; i++) {
                    if (randomBoolean()) {
                        String delId = Integer.toString(i);
                        engine.delete(new Engine.Delete(delId, newUid(delId), primaryTerm.get()));
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                }
                Engine.Searcher external = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL);
                Engine.Searcher internal = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                assertSame(external.getIndexReader(), internal.getIndexReader());
                assertThat(external.getIndexReader(), instanceOf(DirectoryReader.class));
                DirectoryReader dirReader = external.getDirectoryReader();
                ElasticsearchDirectoryReader esReader = getElasticsearchDirectoryReader(dirReader);
                IndexReader.CacheHelper helper = esReader.getReaderCacheHelper();
                assertNotNull(helper);
                assertEquals(helper.getKey(), dirReader.getReaderCacheHelper().getKey());

                IOUtils.close(external, internal);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
            }
            // Close and reopen the main engine
            try (InternalEngine recoveringEngine = new InternalEngine(config)) {
                recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
            }
        } finally {
            IOUtils.close(readOnlyEngine);
        }
    }

    public void testEnsureMaxSeqNoIsEqualToGlobalCheckpoint() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            final int numDocs = scaledRandomIntBetween(10, 100);
            try (InternalEngine engine = createEngine(config)) {
                long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                                                  System.nanoTime(), -1, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                    maxSeqNo = engine.getProcessedLocalCheckpoint();
                }
                engine.syncTranslog();
                globalCheckpoint.set(engine.getPersistedLocalCheckpoint() - 1);
                engine.flushAndClose();

                IllegalStateException exception = expectThrows(IllegalStateException.class,
                                                               () -> new ReadOnlyEngine(config, null, null, true, Function.identity()) {
                                                                   @Override
                                                                   protected boolean assertMaxSeqNoEqualsToGlobalCheckpoint(final long maxSeqNo, final long globalCheckpoint) {
                                                                       // we don't want the assertion to trip in this test
                                                                       return true;
                                                                   }
                                                               });
                assertThat(exception.getMessage(), equalTo("Maximum sequence number [" + maxSeqNo
                                                           + "] from last commit does not match global checkpoint [" + globalCheckpoint.get() + "]"));
            }
        }
    }

    public void testTranslogStats() throws IOException {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            final boolean softDeletesEnabled = config.getIndexSettings().isSoftDeleteEnabled();
            final int numDocs = frequently() ? scaledRandomIntBetween(10, 200) : 0;
            int uncommittedDocs = 0;

            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                                                  System.nanoTime(), -1, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                    globalCheckpoint.set(i);
                    if (rarely()) {
                        engine.flush();
                        uncommittedDocs = 0;
                    } else {
                        uncommittedDocs += 1;
                    }
                }

                assertThat(engine.getTranslogStats().estimatedNumberOfOperations(),
                           equalTo(softDeletesEnabled ? uncommittedDocs : numDocs));
                assertThat(engine.getTranslogStats().getUncommittedOperations(), equalTo(uncommittedDocs));
                assertThat(engine.getTranslogStats().getTranslogSizeInBytes(), greaterThan(0L));
                assertThat(engine.getTranslogStats().getUncommittedSizeInBytes(), greaterThan(0L));

                engine.flush(true, true);
            }

            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, null, true, Function.identity())) {
                assertThat(readOnlyEngine.getTranslogStats().estimatedNumberOfOperations(), equalTo(softDeletesEnabled ? 0 : numDocs));
                assertThat(readOnlyEngine.getTranslogStats().getUncommittedOperations(), equalTo(0));
                assertThat(readOnlyEngine.getTranslogStats().getTranslogSizeInBytes(), greaterThan(0L));
                assertThat(readOnlyEngine.getTranslogStats().getUncommittedSizeInBytes(), greaterThan(0L));
            }
        }
    }
}
