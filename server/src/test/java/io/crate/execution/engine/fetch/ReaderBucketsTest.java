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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;

import io.crate.breaker.CellsSizeEstimator;
import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.RowN;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.InputColumn;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;

public class ReaderBucketsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_reader_bucket_accounts_memory_for_added_rows() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table t1 (x text)")
            .build();
        var t1 = e.resolveTableInfo("t1");
        var x = (Reference) e.asSymbol("x");
        var fetchSource = new FetchSource();
        fetchSource.addFetchIdColumn(new InputColumn(0, DataTypes.LONG));
        fetchSource.addRefToFetch(x);
        var fetchRows = FetchRows.create(
            CoordinatorTxnCtx.systemTransactionContext(),
            TestingHelpers.createNodeContext(),
            Map.of(t1.ident(), fetchSource),
            List.of(
                new FetchReference(new InputColumn(0, DataTypes.LONG), x),
                new InputColumn(1, DataTypes.INTEGER)
            )
        );
        var bytesAccounted = new AtomicLong();
        var ramAccounting = new BlockBasedRamAccounting(
            bytes -> bytesAccounted.addAndGet(bytes),
            1024
        );
        int readerId = 1;
        var readerBuckets = new ReaderBuckets(
            fetchRows,
            reader -> fetchSource,
            new CellsSizeEstimator(List.of(DataTypes.LONG, DataTypes.INTEGER)),
            ramAccounting
        );
        long fetchId = FetchId.encode(readerId, 1);
        readerBuckets.add(new RowN(fetchId, 42));

        assertThat(bytesAccounted.get(), is(1024L));
        assertThat(readerBuckets.ramBytesUsed(), is(40L));

        IntObjectHashMap<Bucket> bucketsByReader = new IntObjectHashMap<>();
        bucketsByReader.put(readerId, new CollectionBucket(List.<Object[]>of(
            new Object[] {"I eat memory for breakfast"}
        )));
        IntHashSet readerIds = new IntHashSet(2);
        readerIds.add(readerId);
        readerBuckets.generateToFetch(readerIds);
        try (var outputRows = readerBuckets.getOutputRows(List.of(bucketsByReader))) {
            assertThat(bytesAccounted.get(), is(1024L));
            assertThat(readerBuckets.ramBytesUsed(), is(136L));
        }

        assertThat(
            "After outputRows are closed the readerBuckets are released",
            readerBuckets.ramBytesUsed(),
            is(0L)
        );
    }
}
