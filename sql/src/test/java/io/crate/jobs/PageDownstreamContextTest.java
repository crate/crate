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
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.BucketPage;
import io.crate.core.collections.Row1;
import io.crate.core.collections.SingleRowBucket;
import io.crate.operation.PageConsumeListener;
import io.crate.operation.PageDownstream;
import io.crate.operation.PageResultListener;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.*;

public class PageDownstreamContextTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

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
        }).when(pageDownstream).fail((Throwable)notNull());

        PageDownstreamContext ctx = new PageDownstreamContext("dummy", pageDownstream, new Streamer[0], RAM_ACCOUNTING_CONTEXT, 3, mock(FlatProjectorChain.class));

        PageResultListener pageResultListener = mock(PageResultListener.class);
        ctx.setBucket(1, new SingleRowBucket(new Row1("foo")), false, pageResultListener);
        ctx.setBucket(1, new SingleRowBucket(new Row1("foo")), false, pageResultListener);

        Throwable t = ref.get();
        assertThat(t, instanceOf(IllegalStateException.class));
        assertThat(t.getMessage(), is("May not set the same bucket of a page more than once"));
    }

    @Test
    public void testKill() throws Exception {
        PageDownstream downstream = mock(PageDownstream.class);
        ContextCallback callback = mock(ContextCallback.class);

        PageDownstreamContext ctx = new PageDownstreamContext("dummy", downstream, new Streamer[0], RAM_ACCOUNTING_CONTEXT, 3, mock(FlatProjectorChain.class));
        ctx.addCallback(callback);
        ctx.kill();

        verify(callback, times(1)).onClose(any(CancellationException.class), anyLong());
        ArgumentCaptor<CancellationException> e = ArgumentCaptor.forClass(CancellationException.class);
        verify(downstream, times(1)).fail(e.capture());
        assertThat(e.getValue(), instanceOf(CancellationException.class));
    }

    @Test
    public void testActiveWhileProcessingPage() throws Exception {
        PageDownstream downstream = mock(PageDownstream.class);
        final SettableFuture<Void> pageCompleteFuture = SettableFuture.create();
        final CountDownLatch pageConsumeLatch = new CountDownLatch(1);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                BucketPage bucketPage = (BucketPage)invocation.getArguments()[0];
                final PageConsumeListener listener = (PageConsumeListener)invocation.getArguments()[1];
                Futures.addCallback(
                        Futures.allAsList(bucketPage.buckets()),
                        new FutureCallback<List<Bucket>>() {
                            @Override
                            public void onSuccess(List<Bucket> result) {
                                try {
                                    pageConsumeLatch.await();
                                    listener.finish();
                                } catch (InterruptedException e) {
                                    fail("interrupted while waiting on page result");
                                }
                                pageCompleteFuture.set(null);
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                pageCompleteFuture.setException(t);
                                listener.finish();
                            }
                        }
                );
                return null;
            }
        }).when(downstream).nextPage(any(BucketPage.class), any(PageConsumeListener.class));
        final PageResultListener resultListener = mock(PageResultListener.class);
        final PageDownstreamContext ctx = new PageDownstreamContext("dummy", downstream, new Streamer[0], RAM_ACCOUNTING_CONTEXT, 2, mock(FlatProjectorChain.class));


        // check passive by default
        assertThat(ctx.subContextMode(), is(ExecutionSubContext.SubContextMode.PASSIVE));

        // set first bucket
        ctx.setBucket(0, new SingleRowBucket(new Row1("foo")), false, resultListener);

        // check active until page is set
        assertThat(ctx.subContextMode(), is(ExecutionSubContext.SubContextMode.ACTIVE));

        // set second bucket
        // in a thread because we block in the consumelistener to check the state while consuming
        Thread setBucketThread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                ctx.setBucket(1, new SingleRowBucket(new Row1("bar")), true, resultListener);
            }
        });
        setBucketThread2.start();

        // check active while page is processed
        assertThat(ctx.subContextMode(), is(ExecutionSubContext.SubContextMode.ACTIVE));
        pageConsumeLatch.countDown();

        setBucketThread2.join(100);
        Thread.sleep(100);
        // check closed is back to passive
        assertThat(ctx.subContextMode(), is(ExecutionSubContext.SubContextMode.PASSIVE));
    }
}