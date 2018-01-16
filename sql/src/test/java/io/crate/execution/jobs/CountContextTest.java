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

package io.crate.execution.jobs;

import io.crate.analyze.WhereClause;
import io.crate.exceptions.UnhandledServerException;
import io.crate.operation.count.CountOperation;
import io.crate.test.CauseMatcher;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingRowConsumer;
import org.elasticsearch.index.Index;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CountContextTest extends CrateUnitTest {

    @Test
    public void testClose() throws Exception {

        CompletableFuture<Long> future = new CompletableFuture<>();

        CountOperation countOperation = mock(CountOperation.class);
        when(countOperation.count(anyMap(), any(WhereClause.class))).thenReturn(future);

        CountContext countContext = new CountContext(1, countOperation, new TestingRowConsumer(), null, WhereClause.MATCH_ALL);
        countContext.prepare();
        countContext.start();
        future.complete(1L);
        assertTrue(countContext.isClosed());
        // assure that there was no exception
        countContext.completionFuture().get();

        // on error
        future = new CompletableFuture<>();
        when(countOperation.count(anyMap(), any(WhereClause.class))).thenReturn(future);

        countContext = new CountContext(2, countOperation, new TestingRowConsumer(), null, WhereClause.MATCH_ALL);
        countContext.prepare();
        countContext.start();
        future.completeExceptionally(new UnhandledServerException("dummy"));
        assertTrue(countContext.isClosed());
        expectedException.expectCause(CauseMatcher.cause(UnhandledServerException.class));
        countContext.completionFuture().get();
    }

    @Test
    public void testKillOperationFuture() throws Exception {
        CompletableFuture<Long> future = mock(CompletableFuture.class);
        CountOperation countOperation = new FakeCountOperation(future);

        CountContext countContext = new CountContext(1, countOperation, new TestingRowConsumer(), null, WhereClause.MATCH_ALL);

        countContext.prepare();
        countContext.start();
        countContext.kill(null);

        verify(future, times(1)).cancel(true);
        assertTrue(countContext.isClosed());
    }

    private static class FakeCountOperation implements CountOperation {

        private final CompletableFuture<Long> future;

        public FakeCountOperation(CompletableFuture<Long> future) {
            this.future = future;
        }

        @Override
        public CompletableFuture<Long> count(Map<String, ? extends Collection<Integer>> indexShardMap,
                                             WhereClause whereClause) throws IOException, InterruptedException {
            return future;
        }

        @Override
        public long count(Index index, int shardId, WhereClause whereClause) throws IOException, InterruptedException {
            return 0;
        }
    }
}
