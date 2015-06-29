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

import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Row1;
import io.crate.core.collections.SingleRowBucket;
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

import java.util.concurrent.CancellationException;
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
}