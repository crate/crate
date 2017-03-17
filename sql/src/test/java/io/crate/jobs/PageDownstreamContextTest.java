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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row1;
import io.crate.core.collections.SingleRowBucket;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageResultListener;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.Loggers;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.*;

public class PageDownstreamContextTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    @Test
    public void testCantSetSameBucketTwiceWithoutReceivingFullPage() throws Exception {
        final AtomicReference<Throwable> ref = new AtomicReference<>();

        PageDownstream pageDownstream = mock(PageDownstream.class);
        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ref.set((Throwable) invocation.getArguments()[0]);
                return null;
            }
        }).when(pageDownstream).fail((Throwable) notNull());

        PageBucketReceiver ctx = new PageDownstreamContext(Loggers.getLogger(PageDownstreamContext.class), "n1",
            1, "dummy", pageDownstream, new Streamer[0], RAM_ACCOUNTING_CONTEXT, 3);

        PageResultListener pageResultListener = mock(PageResultListener.class);
        ctx.setBucket(1, new SingleRowBucket(new Row1("foo")), false, pageResultListener);
        ctx.setBucket(1, new SingleRowBucket(new Row1("foo")), false, pageResultListener);

        Throwable t = ref.get();
        assertThat(t, instanceOf(IllegalStateException.class));
        assertThat(t.getMessage(), is("Same bucket of a page set more than once. node=n1 method=setBucket phaseId=1 bucket=1"));
    }

    @Test
    public void testKillCallsDownstream() throws Exception {
        PageDownstream downstream = mock(PageDownstream.class);

        PageDownstreamContext ctx = new PageDownstreamContext(Loggers.getLogger(PageDownstreamContext.class), "n1",
            1, "dummy", downstream, new Streamer[0], RAM_ACCOUNTING_CONTEXT, 3);

        final AtomicReference<Throwable> throwable = new AtomicReference<>();

        Futures.addCallback(ctx.completionFuture(), new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {

            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                assertTrue(throwable.compareAndSet(null, t));

            }
        });

        ctx.kill(null);
        assertThat(throwable.get(), Matchers.instanceOf(InterruptedException.class));
        verify(downstream, times(1)).kill(any(InterruptedException.class));
    }

    @Test
    public void testSetBucketOnAKilledCtxReleasesListener() throws Exception {
        PageDownstream downstream = mock(PageDownstream.class);
        PageDownstreamContext ctx = new PageDownstreamContext(Loggers.getLogger(PageDownstreamContext.class), "n1",
            1, "dummy", downstream, new Streamer[0], RAM_ACCOUNTING_CONTEXT, 3);
        ctx.kill(new InterruptedException("killed"));

        CompletableFuture<Void> listenerReleased = new CompletableFuture<>();
        ctx.setBucket(0, Bucket.EMPTY, false, new PageResultListener() {
            @Override
            public void needMore(boolean needMore) {
                listenerReleased.complete(null);
            }

            @Override
            public int buckedIdx() {
                return 0;
            }
        });

        // Must not timeout
        listenerReleased.get(1, TimeUnit.SECONDS);
    }
}
