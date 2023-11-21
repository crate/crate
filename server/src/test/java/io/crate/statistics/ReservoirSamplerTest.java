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

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.hppc.LongArrayList;

import io.crate.breaker.RowCellsAccountingWithEstimators;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.Schemas;
import io.crate.statistics.ReservoirSampler.SearchContext;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.QueryTester;

public class ReservoirSamplerTest extends CrateDummyClusterServiceUnitTest {

    private ReservoirSampler sampler;
    private RateLimiter rateLimiter;

    @Before
    public void init() {
        rateLimiter = spy(new RateLimiter.SimpleRateLimiter(0));

        sampler = new ReservoirSampler(
            clusterService,
            createNodeContext(),
            mock(Schemas.class),
            new NoneCircuitBreakerService(),
            mock(IndicesService.class),
            rateLimiter
        );
    }

    @Test
    public void test_rate_limiter_pause_not_called_if_throttling_disabled() throws Throwable {
        QueryTester.Builder builder = new QueryTester.Builder(
            createTempDir(),
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (x int)"
        );
        builder.indexValues("x", 1, 2, 3, 4, 5, 6, 7);
        try (var tester = builder.build()) {
            ArrayList<SearchContext> searchers = new ArrayList<>(1);
            IndexSearcher searcher = tester.searcher();
            searchers.add(new SearchContext(
                searcher,
                List.of(Literal.of("dummy")),
                List.of()
            ));
            RowCellsAccountingWithEstimators rowAccounting = mock(RowCellsAccountingWithEstimators.class);
            // FetchId.decodeReaderId(123) returns 0, second List should have an element with index 0.
            LongArrayList fetchIds = new LongArrayList();
            long fetchId = 123L;
            fetchIds.add(fetchId);
            ArrayList<Row> samples = sampler.createRecords(
                fetchIds,
                searchers,
                RamAccounting.NO_ACCOUNTING,
                rowAccounting,
                1
            );
            assertThat(samples).hasSize(1);
        }
        verify(rateLimiter, never()).pause(anyLong());
    }

    @Test
    public void test_rate_limiter_pause_called_if_throttling_enabled() throws Exception {
        RowCellsAccountingWithEstimators rowAccounting = mock(RowCellsAccountingWithEstimators.class);
        long bytesPerSec = 2;
        long rowSize = bytesPerSec + 1; // ensure 1 pause
        when(rowAccounting.accountRowBytes(any())).thenReturn(rowSize);

        QueryTester.Builder builder = new QueryTester.Builder(
            createTempDir(),
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (x int)"
        );
        builder.indexValues("x", 1, 2, 3, 4, 5, 6, 7);
        try (var tester = builder.build()) {
            ClusterSettings clusterSettings = clusterService.getClusterSettings();
            clusterSettings.applySettings(
                Settings.builder()
                    .put(TableStatsService.STATS_SERVICE_THROTTLING_SETTING.getKey(), bytesPerSec)
                    .build()
            );

            ArrayList<SearchContext> searchersByReaderId = new ArrayList<>(1);
            IndexSearcher searcher = tester.searcher();
            searchersByReaderId.add(new SearchContext(
                searcher,
                List.of(Literal.of("dummy")),
                List.of()
            ));
            // FetchId.decodeReaderId(123) returns 0, second List should have an element with index 0.
            LongArrayList fetchIds = new LongArrayList();
            long fetchId = 123L;
            fetchIds.add(fetchId);
            ArrayList<Row> samples = sampler.createRecords(
                fetchIds,
                searchersByReaderId,
                RamAccounting.NO_ACCOUNTING,
                rowAccounting,
                1
            );
            assertThat(samples).hasSize(1);
        }
        verify(rateLimiter, times(1)).pause(anyLong());
    }
}
