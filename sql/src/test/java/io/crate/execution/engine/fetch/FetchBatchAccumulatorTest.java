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

package io.crate.execution.engine.fetch;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.engine.fetch.FetchBatchAccumulator;
import io.crate.execution.engine.fetch.FetchOperation;
import io.crate.execution.engine.fetch.FetchProjectorContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class FetchBatchAccumulatorTest {

    private static final Reference ID = new Reference(
        new ReferenceIdent(USER_TABLE_IDENT, "id"),
        RowGranularity.DOC,
        DataTypes.LONG
    );
    private DummyFetchOperation fetchOperation = new DummyFetchOperation();

    @Test
    public void testFetchBatchAccumulatorMultipleFetches() throws Exception {
        FetchBatchAccumulator fetchBatchAccumulator = new FetchBatchAccumulator(
            fetchOperation,
            getFunctions(),
            buildOutputSymbols(),
            buildFetchProjectorContext(),
            2
        );
        fetchBatchAccumulator.onItem(new Row1(1L));
        fetchBatchAccumulator.onItem(new Row1(2L));

        Iterator<? extends Row> result = fetchBatchAccumulator.processBatch(false).get(10, TimeUnit.SECONDS);
        assertThat(result.next().get(0), is(1));
        assertThat(result.next().get(0), is(2));

        fetchBatchAccumulator.onItem(new Row1(3L));
        fetchBatchAccumulator.onItem(new Row1(4L));

        result = fetchBatchAccumulator.processBatch(false).get(10, TimeUnit.SECONDS);
        assertThat(result.next().get(0), is(3));
        assertThat(result.next().get(0), is(4));
    }

    private static List<Symbol> buildOutputSymbols() {
        return Collections.singletonList(new FetchReference(
            new InputColumn(0),
            ID
        ));
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

        Map<TableIdent, FetchSource> tableToFetchSource = new HashMap<>(2);
        FetchSource fetchSource = new FetchSource(Collections.emptyList());
        fetchSource.addFetchIdColumn(new InputColumn(0));
        fetchSource.addRefToFetch(ID);
        tableToFetchSource.put(USER_TABLE_IDENT, fetchSource);

        return new FetchProjectorContext(
            tableToFetchSource,
            nodeToReaderIds,
            readerIndices,
            indexToTable
        );
    }

    private static class DummyFetchOperation implements FetchOperation {

        int numFetches = 0;

        @Override
        public CompletableFuture<IntObjectMap<? extends Bucket>> fetch(String nodeId,
                                                                       IntObjectMap<? extends IntContainer> toFetch,
                                                                       boolean closeContext) {
            numFetches++;
            IntObjectHashMap<Bucket> readerToBuckets = new IntObjectHashMap<>();
            for (IntObjectCursor<? extends IntContainer> cursor : toFetch) {
                List<Object[]> rows = new ArrayList<>();
                for (IntCursor docIdCursor : cursor.value) {
                    rows.add(new Object[]{docIdCursor.value});
                }
                readerToBuckets.put(cursor.key, new CollectionBucket(rows));
            }
            return CompletableFuture.completedFuture(readerToBuckets);
        }
    }
}
