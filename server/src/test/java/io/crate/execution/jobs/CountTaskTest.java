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

package io.crate.execution.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.testing.TestingRowConsumer;
import io.crate.exceptions.JobKilledException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.engine.collect.count.CountOperation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Routing;
import io.crate.metadata.TransactionContext;
import io.crate.planner.distribution.DistributionInfo;

public class CountTaskTest extends ESTestCase {

    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    private static CountPhase countPhaseWithId(int phaseId) {
        return new CountPhase(phaseId,
            new Routing(Collections.emptyMap()),
            Literal.BOOLEAN_TRUE,
            DistributionInfo.DEFAULT_BROADCAST);
    }

    @Test
    public void testClose() throws Exception {
        CompletableFuture<Long> future = new CompletableFuture<>();
        CountOperation countOperation = mock(CountOperation.class);
        when(countOperation.count(eq(txnCtx), any(), any(Symbol.class))).thenReturn(future);

        final CountTask countTask1 = new CountTask(countPhaseWithId(1), txnCtx, countOperation, new TestingRowConsumer(), null);
        countTask1.start();
        future.complete(1L);
        assertThat(countTask1.isClosed()).isTrue();
        // assure that there was no exception
        countTask1.completionFuture().get();

        // on error
        future = new CompletableFuture<>();
        when(countOperation.count(eq(txnCtx), any(), any(Symbol.class))).thenReturn(future);

        final CountTask countTask2 =
            new CountTask(countPhaseWithId(2), txnCtx, countOperation, new TestingRowConsumer(), null);
        countTask2.start();
        future.completeExceptionally(new UnhandledServerException("dummy"));
        assertThat(countTask2.isClosed()).isTrue();

        assertThatThrownBy(() -> countTask2.completionFuture().get())
            .hasCauseExactlyInstanceOf(UnhandledServerException.class)
            .hasMessageContaining("dummy");
    }

    @Test
    public void testKillOperationFuture() throws Exception {
        CompletableFuture<Long> future = new CompletableFuture<>();
        CountOperation countOperation = mock(CountOperation.class);
        when(countOperation.count(any(), any(), any())).thenReturn(future);

        CountTask countTask = new CountTask(countPhaseWithId(1), txnCtx, countOperation, new TestingRowConsumer(), null);

        countTask.start();
        countTask.kill(JobKilledException.of("dummy"));

        assertThat(future.isCancelled()).isTrue();
        assertThat(countTask.isClosed()).isTrue();
    }
}
