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

package io.crate.execution.engine.fetch;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;

import io.crate.breaker.CellsSizeEstimator;
import io.crate.data.ArrayBucket;
import io.crate.data.Bucket;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.InputColumn;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class FetchRowsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_fetch_rows_can_map_inputs_and_buckets_to_outputs() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (x text)")
            .addTable("create table t2 (y text, z int)");
        var t1 = e.resolveTableInfo("t1");
        var x = (Reference) e.asSymbol("x");
        var fetchSource1 = new FetchSource();
        fetchSource1.addFetchIdColumn(new InputColumn(0, DataTypes.LONG));
        fetchSource1.addRefToFetch(x);

        var t2 = e.resolveTableInfo("t2");
        var y = (Reference) e.asSymbol("y");
        var fetchSource2 = new FetchSource();
        fetchSource2.addFetchIdColumn(new InputColumn(1, DataTypes.LONG));
        fetchSource2.addRefToFetch(y);

        var fetchSources = Map.of(
            t1.ident(), fetchSource1,
            t2.ident(), fetchSource2
        );
        var fetchRows = FetchRows.create(
            CoordinatorTxnCtx.systemTransactionContext(),
            createNodeContext(null),
            fetchSources,
            List.of(
                new FetchReference(new InputColumn(0, DataTypes.LONG), x),
                new FetchReference(new InputColumn(1, DataTypes.LONG), y),
                new InputColumn(2, DataTypes.INTEGER)
            )
        );
        long fetchIdRel1 = FetchId.encode(1, 1);
        long fetchIdRel2 = FetchId.encode(2, 1);
        var readerBuckets = new ReaderBuckets(
            fetchRows,
            reader -> reader == 1 ? fetchSource1 : fetchSource2,
            CellsSizeEstimator.constant(0),
            RamAccounting.NO_ACCOUNTING
        );
        IntHashSet readerIds = new IntHashSet(2);
        readerIds.add(1);
        readerIds.add(2);
        readerBuckets.add(new RowN(fetchIdRel1, fetchIdRel2, 42));
        readerBuckets.generateToFetch(readerIds);
        IntObjectHashMap<Bucket> results = new IntObjectHashMap<>();
        results.put(1, new ArrayBucket($$($("Arthur"))));
        results.put(2, new ArrayBucket($$($("Trillian"))));

        var it = readerBuckets.getOutputRows(List.of(results));
        assertThat(it.hasNext(), is(true));
        var outputRow = it.next();

        assertThat(outputRow.get(0), is("Arthur"));
        assertThat(outputRow.get(1), is("Trillian"));
        assertThat(outputRow.get(2), is(42));
    }
}
