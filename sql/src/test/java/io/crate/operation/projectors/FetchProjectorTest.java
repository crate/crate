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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.operation.projectors.fetch.FetchOperation;
import io.crate.operation.projectors.fetch.FetchProjector;
import io.crate.operation.projectors.fetch.FetchProjectorContext;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import io.crate.types.LongType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

public class FetchProjectorTest extends CrateUnitTest {

    private static final TableIdent USER_TABLE_IDENT = new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "users");
    private ExecutorService executorService;

    @Before
    public void before() throws Exception {
        executorService = Executors.newFixedThreadPool(2);
    }

    @After
    public void after() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testFetchSize() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();

        FetchOperation fetchOperation = mock(FetchOperation.class);
        SettableFuture<IntObjectMap<? extends Bucket>> future = SettableFuture.create();
        when(fetchOperation.fetch(anyString(), any(IntObjectMap.class), anyBoolean()))
            .thenReturn(future);

        int fetchSize = 3;
        FetchProjector fetchProjector = prepareFetchProjector(fetchSize, rowReceiver, fetchOperation);

        IntObjectMap<Bucket> intObjectMap = new IntObjectHashMap<>(2);
        Collection<Object[]> rows = new ArrayList<>(10);

        long i;
        for (i = 1; i <= 10; i++) {
            Row row = new Row1(i);
            rows.add(row.materialize());
            CollectionBucket collectionBucket = new CollectionBucket(rows);
            intObjectMap.put(0 , collectionBucket);

            RowReceiver.Result result = fetchProjector.setNextRow(row);
            if (i % fetchSize == 0) {
                assertThat(result, is(RowReceiver.Result.PAUSE));
                future.set(intObjectMap);

                final SettableFuture<Void> resumeCalled = SettableFuture.create();
                fetchProjector.pauseProcessed(new ResumeHandle() {
                    @Override
                    public void resume(boolean async) {
                        resumeCalled.set(null);
                    }
                });
                resumeCalled.get(1000, TimeUnit.MILLISECONDS);
            } else {
                assertThat(result, is(RowReceiver.Result.CONTINUE));
            }
        }
        assertThat(i, is(11L));
        fetchProjector.finish(RepeatHandle.UNSUPPORTED);

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
        pipe.prepare();
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
        ReferenceInfo referenceInfo = new ReferenceInfo(referenceIdent,
            RowGranularity.DOC,
            LongType.INSTANCE,
            ColumnPolicy.STRICT,
            ReferenceInfo.IndexType.NOT_ANALYZED,
            true);

        Map<TableIdent, FetchSource> tableToFetchSource = new HashMap<>(2);
        FetchSource fetchSource = new FetchSource(Collections.<ReferenceInfo>emptyList(),
            Collections.singletonList(new InputColumn(0)),
            Collections.singletonList(new Reference(referenceInfo))
            );
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
        ReferenceInfo referenceInfo = new ReferenceInfo(referenceIdent,
            RowGranularity.DOC,
            LongType.INSTANCE,
            ColumnPolicy.STRICT,
            ReferenceInfo.IndexType.NOT_ANALYZED,
            true);

        outputSymbols.add(new FetchReference(inputColumn, new Reference(referenceInfo)));
        return outputSymbols;
    }
}
