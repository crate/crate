/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.projectors;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.CollectionBucket;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.operation.projectors.fetch.FetchOperation;
import io.crate.operation.projectors.fetch.FetchProjector;
import io.crate.operation.projectors.fetch.FetchProjectorContext;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowGenerator;
import io.crate.testing.RowSender;
import io.crate.testing.TestingHelpers;
import io.crate.types.LongType;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

public class FetchProjectorTest extends CrateUnitTest {

    private static final TableIdent USER_TABLE_IDENT = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "users");
    private ExecutorService executorService;
    private DummyFetchOperation fetchOperation;

    @Before
    public void before() throws Exception {
        executorService = Executors.newFixedThreadPool(2);
        // dummy FetchOperation that returns buckets for each reader-id where each row contains a column that is the same as the fetchId
        fetchOperation = new DummyFetchOperation();
    }

    @After
    public void after() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testPauseSupport() throws Exception {
        final CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(2);
        int fetchSize = random().nextInt(20);
        FetchProjector fetchProjector = prepareFetchProjector(fetchSize, rowReceiver, fetchOperation);
        final RowSender rowSender = new RowSender(RowGenerator.range(0, 10), fetchProjector, MoreExecutors.directExecutor());
        rowSender.run();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(rowReceiver.rows.size(), is(2));
                assertThat(rowReceiver.numPauseProcessed(), is(1));
            }
        });
        assertThat(rowReceiver.getNumFailOrFinishCalls(), is(0));
        rowReceiver.resumeUpstream(false);
        assertThat(TestingHelpers.printedTable(rowReceiver.result()),
            is("0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n"));
    }

    @Test
    public void testMultipleFetchRequests() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        int fetchSize = 3;
        FetchProjector fetchProjector = prepareFetchProjector(fetchSize, rowReceiver, fetchOperation);
        final RowSender rowSender = new RowSender(RowGenerator.range(0, 10), fetchProjector, MoreExecutors.directExecutor());
        rowSender.run();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(rowSender.numPauses(), is(3));
            }
        });
        assertThat(fetchOperation.numFetches, Matchers.greaterThan(1));

        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(10));
    }


    private FetchProjector prepareFetchProjector(int fetchSize,
                                                 CollectingRowReceiver rowReceiver,
                                                 FetchOperation fetchOperation) {
        FetchProjector pipe =
            new FetchProjector(
                fetchOperation,
                executorService,
                TestingHelpers.getFunctions(),
                buildOutputSymbols(),
                buildFetchProjectorContext(),
                fetchSize
            );
        pipe.downstream(rowReceiver);
        return pipe;
    }

    private FetchProjectorContext buildFetchProjectorContext() {
        Map<String, IntSet> nodeToReaderIds = new HashMap<>(2);
        IntSet nodeReadersNodeOne = new IntHashSet();
        nodeReadersNodeOne.add(0);
        IntSet nodeReadersNodeTwo = new IntHashSet();
        nodeReadersNodeTwo.add(2);
        nodeToReaderIds.put("nodeOne", nodeReadersNodeOne);
        nodeToReaderIds.put("nodeTwo", nodeReadersNodeTwo);

        TreeMap<Integer, String> readerIndices = new TreeMap<>();
        readerIndices.put(0, "t1");

        Map<String, TableIdent> indexToTable = new HashMap<>(1);
        indexToTable.put("t1", USER_TABLE_IDENT);

        ReferenceIdent referenceIdent = new ReferenceIdent(USER_TABLE_IDENT, "id");
        Reference reference = new Reference(referenceIdent,
            RowGranularity.DOC,
            LongType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);

        Map<TableIdent, FetchSource> tableToFetchSource = new HashMap<>(2);
        FetchSource fetchSource = new FetchSource(Collections.<Reference>emptyList(),
            Collections.singletonList(new InputColumn(0)),
            Collections.singletonList(reference));
        tableToFetchSource.put(USER_TABLE_IDENT, fetchSource);

        return new FetchProjectorContext(
            tableToFetchSource,
            nodeToReaderIds,
            readerIndices,
            indexToTable
        );
    }

    private List<Symbol> buildOutputSymbols() {
        List<Symbol> outputSymbols = new ArrayList<>(2);

        InputColumn inputColumn = new InputColumn(0);
        ReferenceIdent referenceIdent = new ReferenceIdent(USER_TABLE_IDENT, "id");
        Reference reference = new Reference(referenceIdent,
            RowGranularity.DOC,
            LongType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);

        outputSymbols.add(new FetchReference(inputColumn, reference));
        return outputSymbols;
    }

    private static class DummyFetchOperation implements FetchOperation {

        int numFetches = 0;

        @Override
        public ListenableFuture<IntObjectMap<? extends Bucket>> fetch(String nodeId, IntObjectMap<? extends IntContainer> toFetch, boolean closeContext) {
            numFetches++;
            IntObjectHashMap<Bucket> readerToBuckets = new IntObjectHashMap<>();
            for (IntObjectCursor<? extends IntContainer> cursor : toFetch) {
                List<Object[]> rows = new ArrayList<>();
                for (IntCursor docIdCursor : cursor.value) {
                    rows.add(new Object[]{docIdCursor.value});
                }
                readerToBuckets.put(cursor.key, new CollectionBucket(rows));
            }
            return Futures.<IntObjectMap<? extends Bucket>>immediateFuture(readerToBuckets);
        }
    }
}
