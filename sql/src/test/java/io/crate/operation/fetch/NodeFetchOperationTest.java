/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.fetch;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.ImmutableList;
import io.crate.breaker.RamAccountingContext;
import io.crate.metadata.Functions;
import io.crate.operation.collect.CollectContextService;
import io.crate.planner.symbol.Reference;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeFetchOperationTest extends CrateUnitTest {

    static ThreadPool threadPool;
    static CollectContextService collectContextService;

    @BeforeClass
    public static void beforeClass() {
        ThreadPoolExecutor threadPoolExecutor = mock(ThreadPoolExecutor.class);
        when(threadPoolExecutor.getPoolSize()).thenReturn(2);
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any(String.class))).thenReturn(threadPoolExecutor);
        collectContextService = new CollectContextService(ImmutableSettings.EMPTY, threadPool);
    }

    @AfterClass
    public static void afterClass() {
        threadPool = null;
        collectContextService = null;
    }

    @Test
    public void testFetchOperationNoJobContext() throws Exception {
        UUID jobId = UUID.randomUUID();
        NodeFetchOperation nodeFetchOperation = new NodeFetchOperation(
                jobId,
                new LongArrayList(),
                ImmutableList.<Reference>of(),
                true,
                collectContextService,
                threadPool,
                mock(Functions.class),
                mock(RamAccountingContext.class));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "No jobCollectContext found for job '%s'", jobId));
        nodeFetchOperation.fetch();
    }

    @Test
    public void testFetchOperationNoLuceneDocCollector() throws Exception {
        UUID jobId = UUID.randomUUID();
        collectContextService.acquireContext(jobId);

        NodeFetchOperation nodeFetchOperation = new NodeFetchOperation(
                jobId,
                LongArrayList.from(0L),
                ImmutableList.<Reference>of(),
                true,
                collectContextService,
                threadPool,
                mock(Functions.class),
                mock(RamAccountingContext.class));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "No lucene collector found for job search context id '%s'", 0));
        nodeFetchOperation.fetch();
    }
}
