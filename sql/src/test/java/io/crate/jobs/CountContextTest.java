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

package io.crate.jobs;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.UnknownUpstreamFailure;
import io.crate.operation.count.CountOperation;
import io.crate.test.CauseMatcher;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingBatchConsumer;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.*;

public class CountContextTest extends CrateUnitTest {

    @Test
    public void testClose() throws Exception {

        SettableFuture<Long> future = SettableFuture.create();

        CountOperation countOperation = mock(CountOperation.class);
        when(countOperation.count(anyMap(), any(WhereClause.class))).thenReturn(future);

        CountContext countContext = new CountContext(1, countOperation, new TestingBatchConsumer(), null, WhereClause.MATCH_ALL);
        countContext.prepare();
        countContext.start();
        future.set(1L);
        assertTrue(countContext.future.closed());
        // assure that there was no exception
        countContext.completionFuture().get();

        // on error
        future = SettableFuture.create();
        when(countOperation.count(anyMap(), any(WhereClause.class))).thenReturn(future);

        countContext = new CountContext(2, countOperation, new TestingBatchConsumer(), null, WhereClause.MATCH_ALL);
        countContext.prepare();
        countContext.start();
        future.setException(new UnknownUpstreamFailure());
        assertTrue(countContext.future.closed());
        expectedException.expectCause(CauseMatcher.cause(UnknownUpstreamFailure.class));
        countContext.completionFuture().get();
    }

    @Test
    public void testKillOperationFuture() throws Exception {
        ListenableFuture<Long> future = mock(ListenableFuture.class);
        CountOperation countOperation = new FakeCountOperation(future);

        CountContext countContext = new CountContext(1, countOperation, new TestingBatchConsumer(), null, WhereClause.MATCH_ALL);

        countContext.prepare();
        countContext.start();
        countContext.kill(null);

        verify(future, times(1)).cancel(true);
        assertTrue(countContext.future.closed());
    }

    private static class FakeCountOperation implements CountOperation {

        private final ListenableFuture<Long> future;

        public FakeCountOperation(ListenableFuture<Long> future) {
            this.future = future;
        }

        @Override
        public ListenableFuture<Long> count(Map<String, ? extends Collection<Integer>> indexShardMap,
                                            WhereClause whereClause) throws IOException, InterruptedException {
            return future;
        }

        @Override
        public long count(String index, int shardId, WhereClause whereClause) throws IOException, InterruptedException {
            return 0;
        }
    }
}
