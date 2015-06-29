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
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.Functions;
import io.crate.operation.collect.CollectOperation;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Reference;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
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
    static JobContextService jobContextService;

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    @BeforeClass
    public static void beforeClass() {
        ThreadPoolExecutor threadPoolExecutor = mock(ThreadPoolExecutor.class);
        when(threadPoolExecutor.getPoolSize()).thenReturn(2);
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any(String.class))).thenReturn(threadPoolExecutor);
        jobContextService = new JobContextService(ImmutableSettings.EMPTY, threadPool);
    }

    @AfterClass
    public static void afterClass() {
        threadPool = null;
        jobContextService = null;
    }

    @Test
    public void testFetchOperationNoJobContext() throws Exception {
        UUID jobId = UUID.randomUUID();
        NodeFetchOperation nodeFetchOperation = new NodeFetchOperation(
                jobId,
                1,
                new LongArrayList(),
                ImmutableList.<Reference>of(),
                true,
                jobContextService,
                threadPool,
                mock(Functions.class),
                mock(RamAccountingContext.class));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "JobExecutionContext for job %s not found", jobId));
        nodeFetchOperation.fetch(mock(SingleBucketBuilder.class));
    }

    @Test
    public void testFetchOperationNoLuceneDocCollector() throws Exception {
        UUID jobId = UUID.randomUUID();
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId);
        builder.addSubContext(1, new JobCollectContext(jobId,
                mock(CollectNode.class),
                mock(CollectOperation.class),
                RAM_ACCOUNTING_CONTEXT,
                new CollectingProjector()));
        jobContextService.createOrMergeContext(builder);

        NodeFetchOperation nodeFetchOperation = new NodeFetchOperation(
                jobId,
                1,
                LongArrayList.from(0L),
                ImmutableList.<Reference>of(),
                true,
                jobContextService,
                threadPool,
                mock(Functions.class),
                mock(RamAccountingContext.class));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "No SearchContext found for job search context id '%s'", 0));
        SingleBucketBuilder singleBucketBuilder = new SingleBucketBuilder(new Streamer[0]);
        nodeFetchOperation.fetch(singleBucketBuilder);
    }
}
