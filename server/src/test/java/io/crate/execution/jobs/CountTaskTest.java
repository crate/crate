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
import java.util.concurrent.TimeUnit;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
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
import io.crate.testing.PlainRamAccounting;

public class CountTaskTest extends ESTestCase {

    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    private static CountPhase countPhaseWithId(int phaseId) {
        return new CountPhase(phaseId,
            new Routing(Collections.emptyMap()),
            Literal.BOOLEAN_TRUE,
            DistributionInfo.DEFAULT_BROADCAST,
            false);
    }

    @Test
    public void testClose() throws Exception {
        CompletableFuture<Long> future = new CompletableFuture<>();
        CountOperation countOperation = mock(CountOperation.class);
        when(countOperation.count(eq(txnCtx), any(), any(Symbol.class), eq(false))).thenReturn(future);

        RamAccounting ramAccounting = new PlainRamAccounting();
        TestingRowConsumer consumer = new TestingRowConsumer();
        final CountTask countTask1 = new CountTask(
            countPhaseWithId(1),
            txnCtx,
            countOperation,
            consumer,
            null,
            ramAccounting);

        ramAccounting.addBytes(42L);
        countTask1.start();
        assertThat(countTask1.bytesUsed()).isEqualTo(42L);
        future.complete(1L);
        assertThat(countTask1.isClosed()).isTrue();
        assertThat(countTask1.completionFuture()).succeedsWithin(1, TimeUnit.MILLISECONDS);
        assertThat(consumer.completionFuture()).succeedsWithin(1, TimeUnit.MILLISECONDS);

        assertThat(countTask1.bytesUsed())
            .as("closed task preserves total bytes used for sys.operations_log reporting")
            .isEqualTo(42L);
        assertThat(ramAccounting.totalBytes())
            .as("closed RamAccounting releases total bytes")
            .isEqualTo(0L);

        // on error
        future = new CompletableFuture<>();
        when(countOperation.count(eq(txnCtx), any(), any(Symbol.class), eq(false))).thenReturn(future);

        final CountTask countTask2 = new CountTask(
            countPhaseWithId(2),
            txnCtx,
            countOperation,
            new TestingRowConsumer(),
            null,
            RamAccounting.NO_ACCOUNTING
        );
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
        when(countOperation.count(any(), any(), any(), eq(false))).thenReturn(future);

        CountTask countTask = new CountTask(
            countPhaseWithId(1),
            txnCtx,
            countOperation,
            new TestingRowConsumer(),
            null,
            RamAccounting.NO_ACCOUNTING
        );

        countTask.start();
        countTask.kill(JobKilledException.of("dummy"));

        assertThat(future.isCancelled()).isTrue();
        assertThat(countTask.isClosed()).isTrue();
    }
}
